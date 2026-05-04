# ProQue Full Bot Test Plan

Use a test server. Some commands delete messages, change roles/channels, ban/kick users, move money, and edit lottery state.

## Test Accounts

- Superowner: the configured `super_owner_id`
- Current owner: user in the bot owner list
- Current mod: user in the bot mod list
- Normal user: no bot owner/mod power and no Discord admin
- Admin user: Discord Administrator permission, not bot owner/mod
- Server owner: actual Discord server owner

For this round, test the current system as superowner, owner, mod, and normal user. Use the admin/server-owner cases as notes for the next permission refactor.

## Global Checks

- Prefix: `bal` should not run. `.bal` should run unless prefix was changed.
- Prefix command: `.prefix`, `.preifx`, `.prefix !`, then `!help`, `!bal`, and `.bal` should fail after the change.
- Help: `.help`, `.help Quewo category`, `.help <command>`, `.econhelp`, `.quewohelp`, `.explain <command>`.
- Disabled command flow: disable a harmless command, confirm normal/owner/mod cannot use it, confirm superowner can bypass, then re-enable.
- Blacklist flow: blocked user cannot use commands.
- Logs: set log channels, then trigger message delete/edit, role changes, channel changes, member ban/unban/kick, timeout, voice join/move/mute, reaction add/remove/clear.
- Mentions in logs: user identities should be non-pinging mentions, not plain usernames.
- Custom emojis: no command output should show raw custom emoji markdown unless Discord cannot render the emoji.

## Permission Matrix

Expected current behavior before the admin refactor:

- Superowner: can use every owner/mod/superowner command and bypass disabled commands.
- Current owner: can use owner and mod commands, but not superowner-only owner-list commands.
- Current mod: can use owner-or-mod commands only; owner-only commands should fail.
- Normal user: can use public commands only.

After test results, refactor target:

- Superowner highest if present in server.
- Actual server owner higher than admins.
- Discord admins get current owner power.
- Remove bot owner/mod as the main permission model.

## Main Commands

### Public

- `.help`
- `.userinfo [member]`
- `.pfp [member]`
- `.ttt @member`
- `.c4 @member`
- `.chess @member`
- `.move <move>` / `.chessmove <move>` (fallback chess notation)
- `.resign`
- `.q`
- `.testlog`
- `.dsnipe`
- `.esnipe`
- `.rsnipe`
- `.poll <question> | <option> | <option> [time]`
- `.picker ...`
- `.timer <time> [title]`
- `.ctimer`
- `.alarm <date/time>`
- `.calc <expression>`
- `.define <word>`
- `.sleep`
- `.fsleep @member [time]` (currently internally superowner-gated)
- `.wake @member`
- `.afk [reason]`
- `.setbday <dd/mm>`
- `.removebday`
- `.setbdaychannel [channel]`
- `.away`
- `.find ...`
- `.listowners`
- `.listmods`
- `.listtargets`
- `.listcensors`
- `.ask ...`
- `.generate ...`
- `.analyse ...`
- `.translate ...`

### Owner/Mod

- `.disable <command>`
- `.enable <command>`
- `.disableall`
- `.enableall`
- `.dclist`
- `.rolesinfo`
- `.roleinfo <role>`
- `.test`
- `.endttt`
- `.setnick @member <nick>`
- `.purge <amount>`
- `.rpurge <amount>`
- `.unmute @member`
- `.kick @member [reason]`
- `.addrole @member @role`
- `.removerole @member @role`
- `.steal <emoji/sticker>`
- `.giveaway <time> <prize>`
- `.abanlist`
- `.censor <phrase>`
- `.uncensor <phrase>`
- `.clearcensors`
- `.listbans`
- `.listblocks`
- `.lists`

### Owner-Only

- `.setlogs`
- `.deleterole <role>`
- `.shut @member`
- `.unshut @member`
- `.clearwatchlist`
- `.rshut @member`
- `.unrshut @member`
- `.lockdown`
- `.unlock`
- `.rlockdown`
- `.runlock`
- `.lock`
- `.ban @member [reason]`
- `.unban <user/id>`
- `.send [channel] <message>`
- `.reply <message id/link> <message>`
- `.aban @member`
- `.raban @member`
- `.summon @member`
- `.summon2 @member`
- `.block @member`
- `.unblock @member`

### Superowner / Owner-Management

- `.addowner @member`
- `.removeowner @member`
- `.clearowners`
- `.addmod @member`
- `.removemod @member`

### Prefix

- `.prefix`
- `.preifx`
- `.setprefix`

Current expected:

- Superowner in server: only superowner changes prefix.
- Superowner not in server: server owner/admin changes prefix.
- Prefix length 1-5, no spaces, not a user mention.

## Quewo Commands

### Public Quewo

- `.bal [member]` / `.cash [member]`
- `.profile [member]` / `.level [member]` / `.lvl [member]`
- `.quests`
- `.shop`
- `.cooldowns`
- `.transactions [member]`
- `.lottery`
- `.lotterystats`
- `.buytick <amount>`
- `.daily`
- `.weekly`
- `.monthly`
- `.cf <amount> <h/t>`
- `.roulette <amount> [red/black/green]`
- `.slots <amount>`
- `.blackjack <amount>`
- `.scratch <amount>`
- `.ms <amount>`
- `.wheel <amount>`
- `.give @member <amount>`
- `.lb`
- `.econhelp`
- `.quewohelp`
- `.explain <command>`

### Server Owner / Quewo Admin

- `.editlottery <setting> <value>`
- `.stoplottery`

### Quewo Owner-Power / Superowner

- `.add @member|@role|@everyone <amount>`
- `.remove @member <amount|all>`
- `.addtick @member|@role|@everyone <tickets>`
- `.settick @member|@role|@everyone <tickets>`
- `.setquesos @member|@role|@everyone <amount>`

## Feature Tests

### Quewo System

- New user balance row creation.
- Balance formatting with Qoins custom emoji.
- Shop buy flow, item limit, insufficient funds.
- Inventory/profile display.
- Quest claim/refresh buttons.
- Daily/weekly/monthly cooldowns and streaks.
- Gambling cooldowns and result balances.
- Shared Quewo command cooldown blocks rapid back-to-back Quewo commands for normal users.
- Quewo amount parsing: `4m`, `4.5m`, `4k`, `47k`, `734k`, `1b`.
- Slots only pays when all 3 reels match: ×2, ×3, ×4, or ×5 by symbol.
- Scratch only pays when all 5 symbols match, with ×10 payout and low win chance.
- Leaderboard pagination.
- Transactions pagination/limits.
- Chat XP background award and level-up embed.
- Owner/superowner amount override behavior.

### Lottery

- First setup asks for channel and period.
- Lottery channel permissions are applied.
- Panel restores after restart.
- Buy 1/5/10/custom buttons update pot/message.
- Private/ephemeral ticket confirmation works.
- Ticket role assignment.
- `.lotterystats` pagination.
- Minimum-player refund path.
- Winner path pays pot and resets round.
- `.editlottery price/duration/cut/channel`.
- `.stoplottery`.
- `.addtick`, `.settick`, `.setquesos` with user, role, and everyone.

### Games

- TTT challenge accept/decline/timeout.
- TTT bet proposer prompt, opponent bet accept/decline.
- TTT win/draw/timeout payout.
- TTT custom X/O emojis render.
- C4 challenge accept/decline/timeout.
- C4 bet proposer prompt, opponent bet accept/decline.
- C4 board column numbers show below the grid.
- C4 win/draw/timeout payout.
- C4 does not freeze after repeated moves or full columns.
- Chess challenge accept/decline.
- Chess UI only lets the current player move.
- Chess UI source menu lists movable pieces, then legal destination/move choices.
- Chess rejects illegal fallback notation moves.
- Chess detects checkmate, stalemate/draw, and resignation.
- Chess piece custom emoji placeholders should be replaced after Discord markdowns are uploaded.

### Timers, Polls, Giveaway

- Timer create, countdown edit, finish ping, restart restore.
- Timer cancel UI, superowner cancel override.
- Alarm set and final ping.
- Poll create yes/no and multi-option.
- Poll reaction updates.
- Poll timed end and manual `.epoll`.
- Poll restore after restart.
- Giveaway winner path and no-entry path.

### Status Features

- AFK set, mention response, welcome-back summary.
- Sleep set, mention response, wake-up summary.
- Forced sleep/wake permission behavior.
- Birthday set/remove and midnight birthday announcement.
- Birthday channel setup and per-server announcement only when the birthday user is still in that server.
- `.away` output.

### Moderation / Server Tools

- Message delete/edit logs and snipes.
- Purge/rpurge, attachments logging.
- Reaction add/remove/clear logs.
- Reaction shutdown blocks reactions.
- Message shutdown deletes non-owner messages.
- Censor/uncensor/clearcensors blocks messages.
- Shut/unshut deletes watched user messages.
- Role/channel/server update logs.
- Member ban/unban/kick/timeout logs.
- Role add/remove, nick change, lock/unlock.
- Autoban add/remove/list.
- Block/unblock/listblocks.

## Result Format

During live testing, record failures like this:

```text
Role: normal user
Command/feature: .c4 @user
Expected: challenge starts
Actual: bot says ...
Notes: screenshot/log if available
```

Then patch from the failure list.
