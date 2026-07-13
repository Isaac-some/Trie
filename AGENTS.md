# Local-Only Project Guidance

- This is a local CSV processing tool. Do not add Docker, cloud hosting, tunnels, or public network exposure.
- Start it with `npm run local`. The service must keep its default `HOST=127.0.0.1` binding.
- CSV input, generated trees, and exports must remain on the user's computer. Do not upload them or add telemetry.
- The intended setup path is: clone this repository, install Node.js 22 or later, then run the local command.
