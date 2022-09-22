# eventhub-load-tester
Sends JSON payloads to an event hub for a specified time and events per second.

Run with `node .\LoadTest.js -t <minutes> <tps> [-b <batch_size>]`

A `config.json` is required to be in the same path, refer to the `config.template.json` for an example.
