# ProQue Codex Context

This file is the persistent project memory for future Codex sessions. Read this before changing the bot.

## Project

- Repo path: `/Users/majidque/Documents/GitHub/ProQue`
- Main bot file: `main.py`
- Economy module: `economy.py`
- PostgreSQL helpers: `pgdata.py`
- Requirements file: `requirements.txt`
- Emoji asset generator: `tools/generate_q_emojis.py`
- Generated emoji assets:
  - Static PNGs: `assets/emojis/png`
  - Animated GIFs: `assets/emojis/gif`
  - Animation frame PNGs: `assets/emojis/gif_frames`

## Bot Structure

- `main.py` owns the Discord bot instance, command registration, event handling, moderation/tools, AI commands, and imports economy functions.
- `economy.py` owns the economy system, database initialization, economy commands, gambling games, lottery, profile/balance UI, shop, quests, and economy markdown constants.
- `pgdata.py` stores/loads persistent bot config from PostgreSQL.
- `main.py` imports `economy.setup` as `economy_setup` and calls it during bot startup.
- `economy.setup(bot_ref, log_callback=None)` registers economy commands by adding command objects to the bot.

## Current Git State Notes

At the time this file was created, the working tree had uncommitted changes:

- Modified: `economy.py`
- Modified: `main.py`
- Untracked: `assets/`
- Untracked: `tools/`
- New context file: `CODEX_CONTEXT.md`

Do not revert these unless the user explicitly asks.

## Important User Preference

- The user wants future sessions to preserve as much project context as possible.
- When adding bot visuals, prefer the custom Q-themed emoji set over generic Unicode where it improves branding.
- Keep generated emoji files as pure upload-ready image files so the user can upload them to Discord Developer Portal and provide markdown back.
- The user will usually provide Discord custom emoji markdown after uploading. Wire those markdowns into code as constants where possible.
- Whenever adding something new to the bot, check whether it needs a custom Q-themed emoji. If it does, ask whether the user wants one created and tell them to upload it and provide the Discord markdown before wiring it into command UI.

## Currency And Emoji Branding

The main currency is `Qoins`, a shiny blue/silver coin stack with a big `Q`.

Current currency constant in `economy.py`:

```python
CURRENCY_EMOJI = "<:Qoins:1500255107428782100>"
```

The older hard-coded currency markdown was:

```text
<:237D8DDE449D457F9305E100C0CB971F:1500255107428782100>
```

It was replaced with:

```text
<:Qoins:1500255107428782100>
```

`format_balance(amount)` returns:

```python
return f"{amount:,} {CURRENCY_EMOJI}"
```

So most balance/money displays inherit the current currency emoji automatically.

## Uploaded Custom Emoji Markdown

These are defined as constants near the top of `economy.py`.

```python
CURRENCY_EMOJI = "<:Qoins:1500255107428782100>"
QASH_EMOJI = "<:Qash:1500235432011497703>"
Q_DENIED = "<:QDenied:1500427032020914266>"
Q_FLIP = "<:QFlip:1500427033753423993>"
Q_LEVEL_UP = "<:QLevelUp:1500427035292598383>"
Q_MINE = "<:QMine:1500427037301542932>"
QOIN_BAG = "<:QoinBag:1500427038748573777>"
QOIN_CHEST = "<:QoinChest:1500427040212516904>"
QOIN_TRANSFER = "<:QoinTransfer:1500427041735180318>"
Q_QUEST = "<:QQuest:1500427043429417101>"
Q_SHOP = "<:QShop:1500427045019189338>"
Q_SLOTS = "<:QSlots:1500427046365565020>"
Q_SUCCESS = "<:QSuccess:1500427048865235104>"
Q_TIMER = "<:QTimer:1500427051209986188>"
Q_TICKET = "<:QTicket:1500491388985282761>"
Q_WHEEL = "<:QWheel:1500427053160468480>"
Q_XP = "<:QXP:1500427054930333778>"
Q_FLIP_SPIN = "<a:QFlipSpin:1500427305216901160>"
Q_LEVEL_PULSE = "<a:QLevelPulse:1500427307230429284>"
Q_MINE_SPARK = "<a:QMineSpark:1500427308903829586>"
Q_TIMER_TICK = "<a:QTimerTick:1500427311395246180>"
Q_WHEEL_SPIN = "<a:QWheelSpin:1500427313760964691>"
Q_LUCKY_CHARM = "<:QLuckyCharm:1500502953239380089>"
Q_XP_TONIC = "<:QXPTonic:1500502985707618574>"
Q_QUESO_MAGNET = "<:QQuesoMagnet:1500502961162289294>"
Q_DAILY_SPICE = "<:QDailySpice:1500502941927342151>"
Q_STREAK_POLISH = "<:QStreakPolish:1500502970050023445>"
Q_GOLD_BADGE = "<:QGoldBadge:1500502947698577418>"
Q_HIGH_ROLLER = "<:QHighRoller:1500502949594665080>"
Q_VELVET_FRAME = "<:QVelvetFrame:1500502979306852484>"
Q_TICKET_CHARM = "<:QTicketCharm:1500502975746146356>"
Q_COOLDOWN_CLOCK = "<:QCooldownClock:1500502940107149403>"
Q_ROYAL_CROWN = "<:QRoyalCrown:1500502964048232570>"
Q_ACCEPT = "<:QAccept:1500516711114477709>"
Q_ALARM = "<:QAlarm:1500516713094054008>"
Q_ATTACHMENT = "<:QAttachment:1500516714641887402>"
Q_BELL = "<:QBell:1500516716344639618>"
Q_BIRTHDAY = "<:QBirthday:1500516717976097004>"
Q_BOOK = "<:QBook:1500516719771385926>"
Q_BROOM = "<:QBroom:1500516722170396772>"
Q_CARDS = "<:QCards:1500516723860701395>"
Q_CONFETTI = "<:QConfetti:1500516725618118736>"
Q_CONNECT_WHITE = "<:QConnectWhite:1500516729384603708>"
Q_CONNECT_BLACK = "<:QConnectBlack:1500516727547498706>"
Q_EDIT = "<:QEdit:1500516736942866653>"
Q_GAME_O = "<:QGameO:1500516742865227778>"
Q_GAME_TIMEOUT = "<:QGameTimeout:1500516745088077864>"
Q_GAME_WIN = "<:QGameWin:1500516747369910302>"
Q_GAME_X = "<:QGameX:1500516749781504041>"
Q_GIFT = "<:QGift:1500516751467872326>"
Q_HAMMER = "<:QHammer:1500516755301335161>"
Q_IMAGE = "<:QImage:1500516761097863348>"
Q_LOCK = "<:QLock:1500516764369424454>"
Q_PERMISSIONS = "<:QPermissions:1500516773475123412>"
Q_POLL = "<:QPoll:1500516775182336050>"
Q_REACTION = "<:QReaction:1500516779355668573>"
Q_REJECT = "<:QReject:1500516781931106344>"
Q_ROLES = "<:QRoles:1500516783570948330>"
Q_SLEEP = "<:QSleep:1500516788926939220>"
Q_STREAK_FIRE = "<:QStreakFire:1500516793020711035>"
Q_TARGET = "<:QTarget:1500516799710761021>"
Q_THINKING = "<:QThinking:1500516802008973505>"
Q_TIMEOUT = "<:QTimeout:1500516806119522610>"
Q_TRASH = "<:QTrash:1500516810909290716>"
Q_USER_EDIT = "<:QUserEdit:1500516813119946992>"
Q_VOICE = "<:QVoice:1500516816701886535>"
Q_WARNING = "<:QWarning:1500516819604209704>"
```

## Intended Emoji Meanings

- `Qoins`: primary currency coin stack.
- `Qash`: cash stack. Use for balance/profile/rich money contexts.
- `QDenied`: errors, invalid usage, losses, permission denied, failure states.
- `QSuccess`: success actions, wins, purchases, confirmations.
- `QTimer`: static cooldown/timer/cancel messages.
- `QTimerTick`: animated cooldown messages.
- `QTicket`: dedicated lottery ticket emoji for ticket instructions, ticket counts, ticket thread, ticket price, top ticket holders, and `.buytick` purchase messages.
- `QFlip`: coinflip static header/selection/result.
- `QFlipSpin`: animated coinflip spin.
- `QWheel`: wheel/roulette static header/result.
- `QWheelSpin`: animated wheel/roulette spin.
- `QSlots`: slot-machine command/spinning/result header.
- `QMine`: minesweeper bomb/mine and mine hunt setup.
- `QMineSpark`: animated mine hunt header.
- `QXP`: XP, safe mine tile, profile XP field.
- `QLevelUp`: profile level field.
- `QLevelPulse`: animated level-up message in `main.py`.
- `QQuest`: quest embeds and main quest completion.
- `QShop`: shop embed title.
- `QoinBag`: daily/weekly/monthly claim reward.
- `QoinChest`: lottery, jackpots, leaderboard, scratch cards, big-prize contexts.
- `QoinTransfer`: transactions and `.give`.
- Item-specific constants have uploaded Discord markdown. `SHOP_ITEMS` shows them in shop lists, select menus, buy confirmations, and profile inventory.
- `QTicketCharm`: item icon for Ticket Charm.
- `QCooldownClock`: item icon for Cooldown Clock.
- `QRoyalCrown`: item icon for Royal Q Crown.
- Broad `QAccept`/`QReject`/`QWarning`/`QBook`/tool/moderation/game constants are imported into `main.py` with `economy_q_*` aliases and used in user-facing embeds/messages where practical.
- Numeric reaction controls, card suits, slot symbols, roulette colors, scratch symbols, minesweeper cells, and wheel segment colors may still use Unicode because they function as compact game state or reaction selectors unless custom markdown exists for a full replacement set.

## Economy Help And Lottery Refunds

- `.econhelp` / `.economyhelp` / `.ehelp` is an economy-only help embed. It lists commands, aliases, and short explanations, and points users to `.explain <command>` for deeper help.
- Lottery ticket rows track both `spent` and `pot_add` so the bot can refund exact user spend when a lottery round is cancelled.
- If a lottery draw has fewer than 5 unique players, `process_lottery_draw` refunds users automatically, resets the round, and sends a cancellation/refund confirmation message.

## Generated Emoji Assets

Created in this repo with `tools/generate_q_emojis.py`.

Static PNGs, 128x128 RGBA with transparency:

```text
assets/emojis/png/QDenied.png
assets/emojis/png/QDailySpice.png
assets/emojis/png/QAccept.png
assets/emojis/png/QAlarm.png
assets/emojis/png/QAttachment.png
assets/emojis/png/QBell.png
assets/emojis/png/QBirthday.png
assets/emojis/png/QBook.png
assets/emojis/png/QBroom.png
assets/emojis/png/QCards.png
assets/emojis/png/QConfetti.png
assets/emojis/png/QConnectBlack.png
assets/emojis/png/QConnectWhite.png
assets/emojis/png/QFlip.png
assets/emojis/png/QGameO.png
assets/emojis/png/QGameTimeout.png
assets/emojis/png/QGameWin.png
assets/emojis/png/QGameX.png
assets/emojis/png/QGift.png
assets/emojis/png/QGoldBadge.png
assets/emojis/png/QHammer.png
assets/emojis/png/QHighRoller.png
assets/emojis/png/QImage.png
assets/emojis/png/QLevelUp.png
assets/emojis/png/QLock.png
assets/emojis/png/QLuckyCharm.png
assets/emojis/png/QMine.png
assets/emojis/png/QPermissions.png
assets/emojis/png/QPoll.png
assets/emojis/png/QQuesoMagnet.png
assets/emojis/png/QQuest.png
assets/emojis/png/QReaction.png
assets/emojis/png/QReject.png
assets/emojis/png/QRoles.png
assets/emojis/png/QRoyalCrown.png
assets/emojis/png/QShop.png
assets/emojis/png/QSleep.png
assets/emojis/png/QSlots.png
assets/emojis/png/QStreakFire.png
assets/emojis/png/QStreakPolish.png
assets/emojis/png/QSuccess.png
assets/emojis/png/QTarget.png
assets/emojis/png/QThinking.png
assets/emojis/png/QTimer.png
assets/emojis/png/QTicket.png
assets/emojis/png/QTicketCharm.png
assets/emojis/png/QCooldownClock.png
assets/emojis/png/QTimeout.png
assets/emojis/png/QTrash.png
assets/emojis/png/QUserEdit.png
assets/emojis/png/QWheel.png
assets/emojis/png/QXP.png
assets/emojis/png/QXPTonic.png
assets/emojis/png/QVelvetFrame.png
assets/emojis/png/QVoice.png
assets/emojis/png/QWarning.png
assets/emojis/png/QoinBag.png
assets/emojis/png/QoinChest.png
assets/emojis/png/QoinTransfer.png
```

Animated GIFs, 128x128 GIF89a with transparency:

```text
assets/emojis/gif/QFlipSpin.gif
assets/emojis/gif/QLevelPulse.gif
assets/emojis/gif/QMineSpark.gif
assets/emojis/gif/QTimerTick.gif
assets/emojis/gif/QWheelSpin.gif
```

Frame folders:

```text
assets/emojis/gif_frames/QFlipSpin/
assets/emojis/gif_frames/QLevelPulse/
assets/emojis/gif_frames/QMineSpark/
assets/emojis/gif_frames/QTimerTick/
assets/emojis/gif_frames/QWheelSpin/
```

Validation that was performed:

```bash
file assets/emojis/png/*.png assets/emojis/gif/*.gif
python3 -m py_compile economy.py main.py
```

Static PNGs were confirmed as `128 x 128, 8-bit/color RGBA`.
Animated GIFs were confirmed as `128 x 128` and had transparency flags.

## Emoji Generation Details

The AI image generation tool was not exposed in the session, and Pillow was not installed.

Available local tools:

- `python3`
- `ffmpeg` at `/opt/homebrew/bin/ffmpeg`
- `sips` at `/usr/bin/sips`

Unavailable:

- `python` command
- Pillow/PIL
- ImageMagick `magick` / `convert`
- `rsvg-convert`

Because of that, `tools/generate_q_emojis.py` was written as a pure-Python raster generator:

- It draws directly into RGBA pixels.
- It writes PNG files manually via zlib/PNG chunks.
- It generates animation frames.
- `ffmpeg` converts frames into transparent GIFs.

Commands used to generate/rebuild assets:

```bash
python3 tools/generate_q_emojis.py
mkdir -p assets/emojis/gif
ffmpeg -y -framerate 12 -i assets/emojis/gif_frames/QFlipSpin/%03d.png -vf "fps=12,scale=128:128:flags=lanczos,split[s0][s1];[s0]palettegen=reserve_transparent=1:transparency_color=00ff00[p];[s1][p]paletteuse=alpha_threshold=128" assets/emojis/gif/QFlipSpin.gif
ffmpeg -y -framerate 12 -i assets/emojis/gif_frames/QWheelSpin/%03d.png -vf "fps=12,scale=128:128:flags=lanczos,split[s0][s1];[s0]palettegen=reserve_transparent=1:transparency_color=00ff00[p];[s1][p]paletteuse=alpha_threshold=128" assets/emojis/gif/QWheelSpin.gif
ffmpeg -y -framerate 12 -i assets/emojis/gif_frames/QTimerTick/%03d.png -vf "fps=12,scale=128:128:flags=lanczos,split[s0][s1];[s0]palettegen=reserve_transparent=1:transparency_color=00ff00[p];[s1][p]paletteuse=alpha_threshold=128" assets/emojis/gif/QTimerTick.gif
ffmpeg -y -framerate 12 -i assets/emojis/gif_frames/QLevelPulse/%03d.png -vf "fps=12,scale=128:128:flags=lanczos,split[s0][s1];[s0]palettegen=reserve_transparent=1:transparency_color=00ff00[p];[s1][p]paletteuse=alpha_threshold=128" assets/emojis/gif/QLevelPulse.gif
ffmpeg -y -framerate 12 -i assets/emojis/gif_frames/QMineSpark/%03d.png -vf "fps=12,scale=128:128:flags=lanczos,split[s0][s1];[s0]palettegen=reserve_transparent=1:transparency_color=00ff00[p];[s1][p]paletteuse=alpha_threshold=128" assets/emojis/gif/QMineSpark.gif
```

`QFlipSpin.gif` was later improved because the first version did not look enough like a spin. The current generator uses a `flip_coin()` helper where the coin width changes per frame: face-on, edge-on, opposite side, face-on.

Later, a broad replacement PNG pack was generated for remaining normal emoji usage across `main.py`, `pgdata.py`, and leftover `economy.py` spots. These include moderation/log symbols, poll symbols, game/challenge symbols, giveaway/timer/sleep/birthday symbols, and remaining economy symbols. They are upload-ready but not wired until the user provides markdown.

Broad pack names:

```text
QAccept
QAlarm
QAttachment
QBell
QBirthday
QBook
QBroom
QCards
QConfetti
QConnectBlack
QConnectWhite
QEdit
QGameO
QGameTimeout
QGameWin
QGameX
QGift
QHammer
QImage
QLock
QPermissions
QPoll
QReaction
QReject
QRoles
QSleep
QStreakFire
QTarget
QThinking
QTimeout
QTrash
QUserEdit
QVoice
QWarning
```

Recommended wiring targets after upload:

- `QBirthday`: birthday announcement.
- `QHammer`: ban/kick/mod actions.
- `QTrash`: message/channel/role deletion logs.
- `QEdit`: username/nickname/server-name edits.
- `QImage`: server icon/image events.
- `QLock`: verification/security events.
- `QWarning`: ghost ping, invalid input, warnings.
- `QBroom`: purge/bulk delete.
- `QAttachment`: attachment logs.
- `QPoll`, `QAccept`, `QReject`: poll UI and yes/no reactions.
- `QReaction`: reaction logs.
- `QTimeout`: timeout logs.
- `QUserEdit`, `QRoles`, `QVoice`, `QPermissions`: audit/log embeds.
- `QGameWin`, `QGameTimeout`, `QGameX`, `QGameO`, `QConnectBlack`, `QConnectWhite`: tic-tac-toe/connect-four/challenge displays if replacing gameplay symbols.
- `QGift`: giveaways.
- `QAlarm`, `QBell`: timers/alarms.
- `QSleep`: AFK/sleep.
- `QBook`: dictionary/definition output.
- `QThinking`: AI command loading reaction/message.
- `QStreakFire`, `QTarget`, `QCards`, `QConfetti`: leftover economy streak/roulette/blackjack/lottery winner symbols.

## User-Provided Visual References

The user provided:

- `Qoins`: shiny blue/silver coin stack with a big `Q`.
- `Qash`: blue Q-branded cash stack with a white/silver band, big `Q` mark on the cash and band.

Style for future Q emojis:

- Transparent background.
- Icy blue, navy, silver, cyan, white highlights.
- Big readable shapes at Discord emoji size.
- Avoid tiny text except `Q`, `XP`, or very short marks.
- Use high contrast and a centered silhouette.
- Match the shiny blue/silver Q-brand rather than generic emoji style.

## Current Code Changes In Economy

`economy.py` now uses custom emojis in these surfaces:

- Balance/profile embeds.
- Quest embeds and quest claim messages.
- Shop embed and shop buy/error messages.
- Shop items now support an `emoji` field. Use `item_display_name(item)` for message/embed/inventory display and `item_select_emoji(item)` for Discord select-menu option emojis.
- Existing item-specific PNGs were generated for Lucky Charm, XP Tonic, Queso Magnet, Daily Spice, Streak Polish, Gold Badge, High Roller Title, and Velvet Profile Frame.
- Added new shop items:
  - `ticket_charm`: Lottery category, costs 1,200,000, max 5, gives +2% bonus lottery tickets per charm when using `.buytick`.
  - `cooldown_clock`: Utility category, costs 1,500,000, max 5, gives -4% gambling command cooldown per clock.
  - `royal_crown`: Cosmetics category, costs 5,000,000, max 1, upgrades profile title to Royal High Roller.
- Cooldown embed.
- Transactions embed.
- Lottery instructions, setup, updates, stats, ticket purchase, and draw-cancel messages.
- Daily/weekly/monthly claim rewards and cooldown messages.
- Coinflip command:
  - `QFlipSpin` for spinning messages.
  - `QFlip` for static pick/result messages.
  - `QSuccess` and `QDenied` for win/loss.
- Roulette command:
  - `QWheelSpin` for spinning.
  - `QWheel` for color pick/result.
  - Color symbols remain Unicode red/black/green because they represent game outcomes clearly.
- Slots command:
  - `QSlots` for headers and hidden reels.
  - Slot fruit/gem/star symbols remain Unicode because they are actual reel symbols.
  - `QoinChest` for jackpot, `QSuccess` for small win, `QDenied` for no match.
- Blackjack command:
  - Cooldown and usage errors use Q emojis.
  - Win/loss result lines use `QSuccess` / `QDenied`.
  - Card suits remain Unicode because they are actual card state.
- `.give` transfers use `QoinTransfer`.
- Leaderboard title uses `QoinChest`.
- Owner `.add` / `.remove` confirmations and errors use `QSuccess` / `QDenied`.
- Scratch command:
  - `QoinChest` for scratch card headers.
  - `QSuccess` / `QDenied` for win/loss result.
  - Scratch symbols remain Unicode because they are game symbols.
- Minesweeper command:
  - `QMineSpark` for mine hunt header.
  - `QMine` for bomb count and mine setup.
  - `QXP` for safe gem cells.
  - `QDenied` for boom/loss.
  - `QSuccess` for win.
  - Hidden/cursor tiles remain Unicode (`⬛`, `🟨`) because they are grid state.
- Wheel command:
  - `QWheelSpin` while spinning.
  - `QWheel` for landed/static header.
  - Wheel segment color symbols remain Unicode because they represent segment colors.

## Main.py Change

`main.py` imports `Q_LEVEL_PULSE` from `economy.py`:

```python
Q_LEVEL_PULSE as economy_q_level_pulse,
```

It uses it in the chat XP level-up announcement:

```python
f"{economy_q_level_pulse} <@{message.author.id}> reached **level {xp_result['level']}** "
```

This preserves the economy embed and adds Q branding to level-up messages outside `economy.py`.

## Verification

After wiring the emoji markdown into code, syntax checks passed:

```bash
python3 -m py_compile economy.py main.py
```

Earlier, `python -m py_compile economy.py` failed only because `python` is not on PATH. Use `python3`.

## Search Patterns For Future Work

Useful repo-wide searches:

```bash
rg -n "<a?:[A-Za-z0-9_]+:[0-9]+>|emoji|emote|custom emoji|CUSTOM_EMOJI|:[A-Za-z0-9_]+:" .
rg -n "CURRENCY_EMOJI|Q_[A-Z]|QOIN_|QASH|<:Q|<a:Q" economy.py main.py
rg -n "await ctx\\.send|interaction\\.response\\.send_message|discord\\.Embed|edit_message\\(content" economy.py main.py
rg -n "🎉|✅|❌|⏳|⏰|🎟️|🎰|💸|🪙|💎|💣|🎡|🃏|🏆" economy.py main.py
```

When replacing emojis, preserve game-state Unicode symbols if they represent actual gameplay:

- Card suits: `♠️ ♥️ ♦️ ♣️`
- Roulette colors: `🔴 ⚫ 🟢`
- Slot symbols: `🍒 🍋 🍊 🍇 💎 ⭐`
- Scratch symbols: `💎 ⭐ 🔮 🌙 🔥`
- Minesweeper hidden/cursor: `⬛ 🟨`
- Wheel segment colors: `🔴 🔵 🟢 🟠 🟣 🟡 ⬛ 💗`

## Discord Developer Portal Flow

The user uploads files from:

```text
assets/emojis/png
assets/emojis/gif
```

Then they provide markdown like:

```text
<:Name:id>
<a:Name:id>
```

Wire those into `economy.py` as constants, then use constants in bot messages.

For animated emojis in Discord markdown, use the `a:` prefix:

```text
<a:QFlipSpin:1500427305216901160>
```

For static emojis:

```text
<:QoinBag:1500427038748573777>
```

## Known Current Limits

- The generated local PNG/GIF assets are simple stylized raster drawings, not AI-rendered photorealistic images.
- The generated assets intentionally prioritize Discord emoji readability over detail.
- There may be an `.DS_Store` under `assets/emojis/`; ignore or remove later if desired.
- The bot code still contains some Unicode emoji in print logs and in game-state symbols. This is intentional unless the user asks for every last Unicode emoji removed.

## Good Future Practices

- Keep emoji markdown centralized as constants in `economy.py`.
- If other modules need economy emoji constants, import the constant from `economy.py` rather than duplicating markdown.
- Whenever adding something new to the bot, check whether it should have a custom Q-themed emoji. If yes, ask the user whether they want one created and tell them to upload it and provide the markdown before wiring it into live UI.
- For shop items specifically, generate/upload an item emoji when the item is visually distinct enough to help inventory/shop readability.
- Run syntax checks after changes:

```bash
python3 -m py_compile economy.py main.py
```

- Be careful with a dirty working tree. Do not revert user changes.
- Use `rg` for searching.
- Use `apply_patch` for manual edits.
