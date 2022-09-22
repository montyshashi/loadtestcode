/*
 * Copyright (c) 2019 Starbucks Coffee
 * Proprietary and Confidential
 */

const moment = require('moment');
const fs = require('fs');
const { EventHubProducerClient } = require('@azure/event-hubs');
const { exit } = require('process');
let argv = require('minimist')(process.argv.slice(2), {
    alias: {
        t: 'time',
        e: 'events',
        b: 'batchsize'
    }
});


/******** MODIFY FOR SPECIFIC APP/EVENT AS NEEDED ********/
function gen_event() {
    let event = JSON.parse(JSON.stringify(master_event));

    if (users) {
        // If there is a uid/xid csv file being used, this is where the uid is replaced for the event
        event.user_id = get_random_uid();
    }

    return event;
}

function get_users(usersCsv) {
    try {
        let uidXidFile = fs.readFileSync(usersCsv, {
            encoding: 'utf8',
            flag: 'r'
        });
        let uidXids = uidXidFile.split("\n").slice(1);
        
        return uidXids.map(function (uidXid) {
            let user = uidXid.split(",");
            return {uid: user[0], xid: user[1]};
        });
    } catch (err) {
        console.log("Could not read uid+xid csv file '" + usersCsv + "': " + err);
        exit(1);
    }
}

function get_random_uid() {
    return users[Math.floor(Math.random() * users.length)].uid;
}



async function sleep(milliseconds) {
    return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

/**
 * Calculate the number of groups, feeders per group and rate per feeder to sustain
 * a given total event rate.
 *
 * @param {number} aggregate_rate
 * @returns {{rate: number, groups: number, feeders: number}}
 */
function calculate_settings(aggregate_rate) {
    let groups = Math.ceil(aggregate_rate / 400);
    let group_rate = aggregate_rate / groups;
    let feeders = Math.ceil(Math.sqrt(group_rate));
    let rate = Math.ceil(group_rate / feeders);
    return {
        groups,
        feeders,
        rate
    };
}

/**
 *
 * @param {Array} messages
 * @returns {Promise<Object>}
 */
async function process_metrics(messages) {
    let totals = {
        events: 0,
        elapsed: 0,
        failures: 0,
        first_timestamp: null,
        last_timestamp: null,
    };
    messages.forEach(message => {
        let metrics = JSON.parse(message);
        if (metrics) {
            totals.events += metrics.children[0].metrics.items_processed.value;
            totals.failures += metrics.children[0].metrics.requeued.value + metrics.children[0].metrics.failed.value;
            totals.elapsed += metrics.children[0].metrics.processing_time.value;
            if (metrics.children[0].metrics.processing_time.started_at) {
                let t = moment(metrics.children[0].metrics.processing_time.started_at);
                if (!totals.first_timestamp || t.isBefore(totals.first_timestamp)) {
                    totals.first_timestamp = t;
                }
            }
            if (metrics.children[0].metrics.processing_time.ended_at) {
                let t = moment(metrics.children[0].metrics.processing_time.ended_at);
                if (!totals.last_timestamp || t.isBefore(totals.last_timestamp)) {
                    totals.last_timestamp = t;
                }
            }
        }
    });
    return Promise.resolve(totals);
}

/**
 * Feed a stream of events to a target at a constant rate. Given the number of events in the stream
 * and the interval over which to send them, a delay time between events is calculated such that the
 * events will be sent evenly spaced across the entire interval. Eg., if events contains 20 event objects
 * and interval is 1, one event will be sent every 50 milliseconds.
 *
 * @param {Object} client event hub client
 * @param {number} events count of events to send
 * @param {number} interval seconds over which to feed the event stream to the target
 */
async function feeder(client, events, interval, batch_size) {
    let period = (interval / events) * 1000 * batch_size; // Time between event batches in milliseconds
    let events_left = events;
    while (events_left > 0) {
        let start = moment();

        let num_events = batch_size;
        if (events_left < batch_size) {
            num_events = events_left;
        }
        events_left -= num_events;

        let batch = await client.createBatch();

        for (let i of Array(num_events).keys()) {
            let event = gen_event();
            batch.tryAdd({body: event});
        }
        
        try {
            await client.sendBatch(batch);
        } catch (err) {
            console.error('Send error: ' + err.message);
            throw err;
        }
        // Account for time needed to create and send the event
        let delay = period - moment().diff(start);
        if (delay > 0 && events_left > 0) {
            await sleep(delay);
        }
    }
    return null;
}

/**
 * Generates a group of feeder jobs, each running at the specified event rate for
 * the specified interval.
 *
 * @param {number} rate events per second for each feeder to send
 * @param {number} feeders number of parallel feeders to use
 * @param {number} interval seconds over which to feed the event stream to the target
 * @returns {Promise<[(*|undefined)]>}
 */
async function feeder_group(rate, feeders, interval, batch_size) {
    // Create more clients if necessary
    if (feeders > event_hub_clients.length) {
        let needed = feeders - event_hub_clients.length;

        while (needed > 0) {
            event_hub_clients.push(
                new EventHubProducerClient(event_hub_connection_string, event_hub_name)
            );
            needed--;
        }
    }
    let events = rate * interval;
    let offset = 1000 / feeders; // Spread the feeders out across one second
    let promises = [];
    for (let i = 0; i < feeders; i++) {
        let start = moment();
        promises.push(feeder(event_hub_clients[i], events, interval, batch_size));
        if (i < (feeders - 1)) {
            let delay = offset - moment().diff(start);
            if (delay > 0) {
                await sleep(delay);
            }
        }
    }
    return Promise.all(promises);
}

/**
 * Create enough feeder groups to generate the given event rate across the interval
 * specified. Accounts for overhead at both the feeder and feeder group level so that
 * delays due to overhead are minimized.
 *
 * @param {Object} settings
 * @param {number} interval
 * @returns {Promise<[(*|undefined)]>}
 */
async function create_feeders(settings, interval, batch_size) {
    let feeder_groups = [];
    for (let i = 0; i < settings.groups; i++) {
        feeder_groups.push(feeder_group(settings.rate, settings.feeders, interval, batch_size));
    }
    return Promise.all(feeder_groups);
}



/**********************************************************************************/
/* Start of script ****************************************************************/
/**********************************************************************************/



// DO NOT MODIFY, MODIFY THE CONFIG.JSON FILE INSTEAD
let config_json = {
    "event_hub_connection_string": "<event hub connection string>",
    "event_hub_name": "<event hub name>",
    "batch_size": 10,
    "master_event": {},
    "uid_xid_csv": "load_uid_xid.csv",
};
let config_json_pretty = JSON.stringify(config_json, null, 2);

const config_path = "config.json";
let data;
try {
    if (fs.existsSync(config_path)) {
        data = fs.readFileSync(config_path);
    } else {
        console.log("'config.json' configuration file not found! Must be of this form:\n\n" + config_json_pretty);
        exit(1);
    }
} catch(err) {
    console.error(err);
    exit(1);
}

let config;
try {
    config = JSON.parse(data);
} catch (err) {
    console.log("Config is invalid json: " + err);
    exit(1);
}

let uid_xid_csv = config.uid_xid_csv;

const event_hub_connection_string = config.event_hub_connection_string;
const event_hub_name = config.event_hub_name;
const master_event = config.master_event;

let batch_size = parseInt(config.batch_size);

if (!batch_size && batch_size < 1) {
    batch_size = 1;
}

if (!(event_hub_connection_string && event_hub_name && master_event)) {
    console.log("Config invalid. Must be of this form:\n\n" + config_json_pretty);
    exit(1);
}

let event_hub_clients = [];

let users = null;
if (uid_xid_csv) {
    users = get_users(uid_xid_csv);
}

let interval = 0;
let number_of_events = 0;
let rate = 0;

// let number_of_events = parseInt(process.argv[2], 10);
// if (isNaN(number_of_events)) {
//     console.log('Invalid number of events: ' + process.argv[2]);
//     process.exit(1);
// }
// if (number_of_events < 1) {
//     console.log('At least one event is required.');
//     process.exit(1);
// }

if (argv.time) {
    if (typeof argv.time == 'number') {
        interval = argv.time;
        if (isNaN(interval)) {
            console.log('Invalid run time: ' + argv.time);
            process.exit(1);
        }
    } else {
        interval = parseInt(argv.time, 10);
    }
    if (interval < 1) {
        console.log('Run must be at least 1 minute.');
        process.exit(1);
    }
    interval *= 60; // Convert to seconds
}

if (argv.events) {
    if (typeof argv.events == 'number') {
        number_of_events = argv.events;
        if (isNaN(number_of_events)) {
            console.log('Invalid number of events: ' + argv.events);
            process.exit(1);
        }
    } else {
        number_of_events = parseInt(argv.events, 10);
    }
    if (number_of_events < 1) {
        console.log('At least one event is required.');
        process.exit(1);
    }
}

if (argv.batchsize) { // Overwrite the batch_size in the config if it's passed in with the -b flag
    let temp_batch_size;
    if (typeof argv.batchsize == 'number') {
        temp_batch_size = argv.batchsize;
    } else {
        temp_batch_size = parseInt(argv.batch_size, 10);
    }
    if (temp_batch_size < 1 || isNaN(temp_batch_size)) {
        console.log('Batch size must be at least 1. Continuing with setting from config (' + batch_size + ')...');
    } else {
        batch_size = temp_batch_size;
    }
}

if (!number_of_events && !interval) {
    console.log('You must give either the number of events or the run time.');
    process.exit(1);
} else if (number_of_events > 0 && interval > 0) {
    console.log('You may only give the number of events or the run time, not both.');
    process.exit(1);
}

if (argv._.length > 0 && argv._[0]) {
    rate = parseInt(argv._[0], 10);
    if (isNaN(rate) || !rate) {
        console.log('Invalid rate: ' + argv._[0]);
        process.exit(1);
    }
    if (rate < 10 || rate > 8000) {
        console.log('Rate ' + rate.toString() + ' is out of range 10-8000');
        process.exit(1);
    }
} else {
    console.log('No rate provided.');
    process.exit(1);
}

if (number_of_events > 0 && !interval) {
    interval = number_of_events / rate;
}

let estimated_minutes = interval / 60;

let settings = calculate_settings(rate);

let est_min = Math.floor(estimated_minutes);
let est_max = Math.ceil(estimated_minutes);
console.log('Run estimated to take between ' + est_min.toString() + ' and ' + est_max.toString() +
    ' minutes to complete.');
console.log('Using ' + settings.groups.toString() +
    ' groups of ' + settings.feeders.toString() +
    ' feeder jobs running at ' + settings.rate.toString() +
    ' events per second each, with a batch size of ' + batch_size.toString() + 
    ' events per batch.');
console.log('Will send ' + (settings.groups * settings.feeders * settings.rate * interval) + ' events in total.');

let start_time = moment();
create_feeders(settings, interval, batch_size).then(() => {
    let run_time = moment().diff(start_time);
    console.log('Run took ' + (run_time / 60000).toString() + ' minutes at ' +
        ((settings.groups * settings.feeders * settings.rate * interval) / (run_time / 1000)) +
        ' events/second.');
    console.log('Closing...');
    let promises = [];
    event_hub_clients.forEach(client => {
        promises.push(client.close());
    });
    return Promise.all(promises);
}).then(() => {
    console.log('Finished!');
}).catch(error => {
    console.log('Error! ' + error.message);
});