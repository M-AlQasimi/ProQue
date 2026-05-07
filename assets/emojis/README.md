# ProQue Emoji Assets

Use `upload/` when adding emojis to Discord. It is organized by where the emoji is used:

- `upload/png/general` - shared status, confirmation, birthday, timer, and UI emojis.
- `upload/png/quewo` - Quewo currency, shop, XP, lottery, and item emojis.
- `upload/png/moderation` - moderation, logs, roles, permissions, and server tools.
- `upload/png/games` - game-specific emojis split by game.
- `upload/png/polls` - poll number and poll UI emojis.
- `upload/animated` - GIF emojis ready for upload.

Run `python3 tools/generate_q_emojis.py` to regenerate the upload-ready files. The generator writes directly into `upload/` and uses temporary animation frames, so old flat cache folders are not needed.

Some upload files intentionally share the same artwork under different names, such as poll numbers and Connect 4 numbers. Keep those separate because Discord emoji names are part of the bot UI.
