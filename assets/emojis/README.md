# ProQue Emoji Assets

Use `upload/` when adding emojis to Discord. It is organized by where the emoji is used:

- `upload/png/general` - shared status, confirmation, birthday, timer, and UI emojis.
- `upload/png/quewo` - Quewo currency, shop, XP, lottery, and item emojis.
- `upload/png/moderation` - moderation, logs, roles, permissions, and server tools.
- `upload/png/games` - game-specific emojis split by game.
- `upload/png/polls` - poll number and poll UI emojis.
- `upload/animated` - GIF emojis ready for upload.

The flat `png/` folder is the generator output cache. Keep it because `tools/generate_q_emojis.py` writes there for quick regeneration.
