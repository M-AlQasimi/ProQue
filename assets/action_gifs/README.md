ProQue social-action GIFs.

Only files under `_approved/<command>/` are used by the bot.
The old per-command folders are ignored because they contain generated placeholder motion, not production reaction clips.

Each approved subfolder maps to a command name:

- `hug`
- `pat`
- `slap`
- `bonk`
- `kiss`
- `bite`
- `poke`
- `wave`
- `cry`
- `kill`

The bot randomly chooses a `.gif`, `.webp`, or `.mp4` from `_approved/<command>/`.
Generated candidates should go into `_pending/<command>/` first, then only get moved to `_approved/<command>/` after they look like real reaction clips.

Quality bar:

- real frame-to-frame action, not panning a still image
- clear subject motion and reaction timing
- no childish placeholder drawings
- no visible prompt text, watermarks, or broken anatomy
- short enough for Discord embeds to load quickly
