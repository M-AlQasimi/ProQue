# Pro𝚀𝚞𝚎 Full Bot Test Plan

Use a test server. Run this in order: global checks first, then Superowner, Server Owner, Admin, Normal User, then shared feature tests.

Some tests delete messages, move money, change roles/channels, ban/kick users, and edit lottery/activity state.

## Test Accounts

- Superowner: configured `super_owner_id`
- Server owner: actual Discord server owner
- Admin user: has Discord Administrator permission
- Normal user: no Discord admin permissions

Expected permission order: superowner > server owner > Discord admins > normal users.

## Global Setup Checks

Test these once before role testing.

### Prefix

- Send `bal`.
  Expected: command does not run.
- Send `.bal`.
  Expected: command runs.
- Send `.prefix !` as the allowed role for the current server.
  Expected: prefix changes.
- Send `!help` and `!bal`.
  Expected: both run.
- Send `.bal`.
  Expected: does not run after prefix changed.
- Change prefix back to `.` with `!prefix .`.

### Help

- `.help`
- Press Refresh in `.help`.
- `.help 𝚀𝚞𝚎wo`
- `.help activity`
- `.help poll` then press the setup UI button.
- `.econhelp`
- Press category/page/Refresh buttons in `.econhelp`.
- `.quewohelp`
- `.explain lottery`
- `.explain activity`
- `.explain editactivity`
- `.explain settings`
- `.explain jobs`
- `.explain recover`
- `.explain errors`
- `.explain dbaudit`
- `.explain aiguard`
- `.explain games`
- As superowner: `.explain balancedashboard`, `.explain styleaudit`, `.explain commandcleanup`

Expected: all are current, mention the right aliases/settings, have no duplicate entries, and long panels still work after Refresh.

### Slash Commands

- Wait for startup log: `Slash command sync complete`.
- If slash commands still do not appear, re-invite the bot with the `applications.commands` OAuth2 scope.
- `/commands`
- `/help`
- `/help command:poll`
- `/games`
- `/settings` as an admin-power user.
- `/run command:bal`
- `/run command:profile args:@normal`
- `/run command:poll args:Slash poll? yes no 1m`
- `/run command:timer args:1m slash test`
- `/run command:qstats` as an admin-power user.

Expected: `/run` autocomplete lists prefix commands, direct slash commands respond, and slash-run commands behave like their prefix versions. Discord only exposes a few top-level slash entries because all prefix commands are available through `/run`.

### Logs And Mentions

- Run `.setlogs`.
- Delete a message.
- Edit a message.
- Add/remove a role.
- Kick/ban/unban a test user if safe.
- Timeout/unmute a test user if safe.
- Join/move/leave voice.
- Add/remove/clear reactions.

Expected: logs use non-pinging mentions like `<@id>`, not plain usernames/nicknames.

### Custom Emojis

- Run `.bal`, `.shop`, `.inventory`, `.lottery`, `.scratch 1000`, `.tower 1000`, `.vault 1000`, `.memory 1000`, `.cardladder 1000`, `.lockpick 1000`, `.dungeon`, `.slots 1000`, `.roulette 1000 red`, `.ms 1000`, `.wheel 1000`, `.poll Test question? one two`.

Expected: no raw custom emoji markdown appears unless Discord cannot render that emoji.

## Superowner Tests

Expected: can use everything, can bypass disabled commands, and can use all 𝚀𝚞𝚎wo admin tools.

### Public Commands

Run:

- `.help`
- `.userinfo`
- `.userinfo @normal`
- `.pfp`
- `.pfp @normal`
- `.q`
- `.games`
- `.flagquiz` then choose Solo/Public and 10.
- `.flagquiz 20` then choose Solo/Public.
- `.testlog`
- `.testrlog`
- `.dsnipe`
- `.esnipe`
- `.rsnipe`
- `.picker apple banana orange`
- `.picker "ice cream" pizza sushi`
- `.picker` then use the setup UI.
- `.timer 1m test`
- `.timer test 1m`
- `.timer` then use the setup UI.
- `.ctimer`
- `.alarm 1m test alarm`
- `.alarm test alarm 1m`
- `.alarm` then use the setup UI.
- `.calc 2+2*5`
- `.calc` then use the setup UI.
- `.define test`
- `.define` then use the setup UI.
- `.afk testing`
- `.away`
- `.sleep`
- `.wake @normal`
- `.fsleep @normal 1m`
- `.setbday 01/01`
- `.setbday` then use the setup UI.
- `.removebday`
- `.setbdaychannel #channel`
- `.setbdaychannel <channel id>`
- `.activity`
- `.activitystats`
- `.settings`
- `.controlpanel`
- In `.settings` / `.controlpanel`, test Refresh, Prefix, Birthdays Here, Activity Here, Admin Commands, Setup Guide, and Lottery Panel if safe.
- `.perf`
- `.bulkqueue`
- `.jobs`
- `.errors`
- `.dbaudit`
- `.recover`
- `.aiguard`
- `.auditcommands`
- `.styleaudit`
- `.commandcleanup`
- `.gamebalance`
- `.balancedashboard`
- `.balancedashboard 30`
- `.find <normal_user_id>`
- `.listtargets`
- `.listcensors`
- `.ask hello`
- Mention/reply to the bot and ask what it remembers about you.
- `.aimemory`
- `.usersettings`
- `.usersettings aifriendly off`
- `.generate short test prompt`
- Reply to an image with `.analyse`
- Reply to an image with `.analyze`
- `.translate hello to Italian`

Expected: all run. Commands that need external APIs may fail only if their API/config is unavailable.

### Games

Run with another test user:

- `.ttt @normal`
- `.ttt @normal` with a bet
- `.c4 @normal`
- `.c4 @normal` with a bet
- `.chess @normal`
- `.chess @normal` with a bet
- `.move e2e4` or `.chessmove e2e4` during chess fallback testing
- `.resign`
- `.endttt`

Expected: challenges, accept/decline, bet accept/decline, turns, payouts, and timeouts work.

### Redeploy / Restart Recovery

- Start `.ttt @normal`, make at least one move, then restart the bot.
- Start `.c4 @normal`, make at least one move, then restart the bot.
- Start `.chess @normal`, make at least one move, then restart the bot.

Expected: active TTT, C4, and Chess messages are restored with the same players, board state, bet amount, and turn controls. Old unsaved stale game UIs should expire instead of staying clickable.

### Operations / Recovery

- `.health`
- `.perf`
- `.bulkqueue`
- `.jobs`
- `.recover`
- `.jobs` again after recovery finishes.
- `.errors`
- `.dbaudit`
- `.aiguard`

Expected: performance panels respond quickly, recovery starts as a background job, `.jobs` shows running/done state, `.errors` is empty unless failures happened, `.dbaudit` lists blocking DB risks or says none, and `.aiguard` shows AI command safety categories.

### Admin Commands

Run safe versions first:

- `.disable calc`
- `.calc 1+1`
- `.enable calc`
- `.dclist`
- `.rolesinfo`
- `.roleinfo @role`
- `.test`
- `.purge 1` and `.purge @normal 1`
- `.rpurge 1` and `.rpurge @normal 1`
- `.steal` by replying to an emoji/sticker/image
- `.giveaway 1m test prize`
- `.giveaway test prize 1m`
- `.giveaway` then use the setup UI.
- `.listbans`
- `.listblocks`
- `.lists`
- `.censor badtestphrase`
- `.listcensors`
- `.uncensor badtestphrase`
- `.clearcensors`

Run only if safe:

- `.setnick @normal TestName`
- `.unmute @normal`
- `.kick @normal test`
- `.ban @normal test`
- `.unban <normal_user_id>`
- `.addrole @normal @role` and `.addrole @role @normal`
- `.removerole @normal @role` and `.removerole @role @normal`
- `.deleterole @role`
- `.setlogs`
- `.lock`
- `.unlock`
- `.lockdown`
- `.reopen`
- `.rlockdown`
- `.runlock`
- `.shut @normal`
- `.unshut @normal`
- `.rshut @normal`
- `.unrshut @normal`
- `.clearwatchlist`
- `.send #channel test message`
- `.reply <message_id_or_link> test reply`
- `.aban @normal`
- `.raban @normal`
- `.abanlist`
- `.summon @normal`
- `.summon2 @normal`
- `.block @normal`
- `.unblock @normal`

Expected: superowner can run these where Discord/bot role hierarchy allows it.

### Activity Admin

Run:

- `.activity`
- `.activity setup`
- Choose a channel from the dropdown.
- `.activity setup`
- Reply with a channel ID or mention instead of using the dropdown.
- `.activitystats`
- `.editactivity channel #channel`
- `.editactivity next 12h`
- `.endactivity`
- `.stopactivity`
- `.activity`

Expected: setup works both ways, status is an embed, edit commands work, endactivity posts winners and starts a fresh window, stopactivity disables reports and clears current activity.

### 𝚀𝚞𝚎wo Commands

Run:

- `.bal`
- `.cash`
- `.bank`
- `.bank deposit 10k`
- `.bank withdraw 5k`
- `.bank interest`
- `.profile`
- `.level`
- `.lvl`
- `.guide`
- `.streaks`
- `.inventory`
- `.inv`
- `.items`
- `.quests`
- `.dailychallenge`
- `.dailychallenge claim`
- `.shop`
- Buy one Streak Freeze from `.shop`, then verify `.inventory` shows it.
- `.tutorial`
- `.tutorial off`
- `.tutorial on`
- `.cooldowns`
- `.transactions`
- `.daily`
- `.weekly`
- `.monthly`
- `.cf 1000 h`
- `.roulette 1000 red`
- `.slots 1000`
- `.blackjack 1000`
- `.scratch 1000`
- `.tower 1000`
- `.vault 1000`
- `.memory 1000`
- `.cardladder 1000`
- `.lockpick 1000`
- `.heist 1000`
- `.diceduel 1000`
- `.cases 1000`
- `.plinko 1000`
- `.luckynumber 1000`
- `.jackpotspin 1000`
- `.dungeon`
- `.ms 1000`
- `.wheel 1000`
- `.gamestats`
- `.achievements`
- `.setbadge`
- `.setbadge clear`
- `.gamebalance`
- `.gamehistory`
- Press Replay in `.gamehistory`.
- `.seasonpass`
- `.recommendgame`
- `.limits`
- `.flagstats`
- `.give @normal 1000` and `.give 1000 @normal`
- `.lb`
- `.qstats`
- `.economyaudit`
- `.auditcommands`
- `.econhelp`
- `.quewohelp`
- `.explain scratch`

Expected: balances update correctly and cooldown/results make sense. `.gamebalance` includes risk mix and balance checks. `.auditcommands` includes help/explain, duplicate alias, stale explanation, input UI, and sync DB call sections.

### 𝚀𝚞𝚎wo Admin

Run:

- `.lottery`
- `.lotterystats`
- `.editlottery price 200000`
- `.editlottery duration 12h`
- `.editlottery cut 10`
- `.editlottery channel #channel`
- `.robsettings`
- `.robsettings on`
- `.rob @normal`
- `.robsettings off`
- `.buytick 1`
- `.add @normal 1000` and `.add 1000 @normal`
- `.remove @normal 100` and `.remove 100 @normal`
- `.addtick @normal 2` and `.addtick 2 @normal`
- `.settick @normal 5` and `.settick 5 @normal`
- `.setquesos @normal 2500000` and `.setquesos 2500000 @normal`
- `.add @role 1000` and `.add 1000 @role`
- `.add @everyone 1000` and `.add 1000 @everyone`
- `.addtick @role 1` and `.addtick 1 @role`
- `.settick @role 1` and `.settick 1 @role`
- `.setquesos @role 1000` and `.setquesos 1000 @role`
- `.stoplottery`
- `.qstats`
- `.economyaudit`

Expected: all work. Role/everyone operations affect the expected users. Balances/tickets shown after admin changes match `.bal` and `.lotterystats`. Banked money is protected from `.rob`. Audit shows game signals without embed errors.

### Superowner Permission Checks

- Disable `.calc`, then run `.calc 1+1` as superowner.
  Expected: superowner bypasses disabled command.
- Try acting on server owner/admin/normal users.
  Expected: bot allows it where Discord hierarchy allows it.
- Change prefix while superowner is in server.
  Expected: only superowner can change it.

## Server Owner Tests

Expected: can use admin-power commands, outranks admins, cannot bypass disabled commands, and cannot act on superowner.

### Public Commands

Run the same public commands from Superowner Tests, except `.fsleep` should only work if internally allowed.

Expected: public commands work.

### Admin Commands

Run:

- `.disable calc`
- `.enable calc`
- `.disableall`
- `.enableall`
- `.dclist`
- `.rolesinfo`
- `.roleinfo @role`
- `.test`
- `.endttt`
- `.purge 1` and `.purge @normal 1` and confirm/cancel once.
- `.rpurge 1` and `.rpurge @normal 1` and confirm/cancel once.
- `.steal`
- `.giveaway 1m test prize`
- `.giveaway test prize 1m`
- `.giveaway` then use the setup UI.
- `.listbans`
- `.listblocks`
- `.lists`
- `.censor badtestphrase`
- `.uncensor badtestphrase`
- `.clearcensors`
- `.settings`
- `.setlogs`
- `.lock`
- `.unlock`
- `.editactivity channel #channel`
- `.editactivity next 12h`
- `.endactivity`
- `.stopactivity`

Run only if safe:

- `.setnick @admin TestName`
- `.setnick @normal TestName`
- `.kick @normal test`
- `.ban @normal test`
- `.unban <normal_user_id>`
- `.addrole @normal @role` and `.addrole @role @normal`
- `.removerole @normal @role` and `.removerole @role @normal`
- `.deleterole @role`
- `.lockdown`
- `.reopen`
- `.rlockdown`
- `.runlock`
- `.shut @normal`
- `.unshut @normal`
- `.rshut @normal`
- `.unrshut @normal`
- `.clearwatchlist`
- `.send #channel test message`
- `.reply <message_id_or_link> test reply`
- `.aban @normal`
- `.raban @normal`
- `.abanlist`
- `.summon @normal`
- `.summon2 @normal`
- `.block @normal`
- `.unblock @normal`

Expected: server owner can act on admins and normal users, but not superowner.

### 𝚀𝚞𝚎wo Admin

Run:

- `.editlottery price 200000`
- `.editlottery duration 12h`
- `.editlottery cut 10`
- `.editlottery channel #channel`
- `.stoplottery`
- `.add @normal 1000` and `.add 1000 @normal`
- `.remove @normal 100` and `.remove 100 @normal`
- `.addtick @normal 2` and `.addtick 2 @normal`
- `.settick @normal 5` and `.settick 5 @normal`
- `.setquesos @normal 1000` and `.setquesos 1000 @normal`

Expected: works for server owner.

### Server Owner Permission Checks

- Disable `.calc`, then run `.calc 1+1`.
  Expected: denied while disabled.
- Try admin commands against superowner.
  Expected: denied.
- Try admin commands against admin and normal user.
  Expected: allowed where Discord hierarchy allows it.
- Try `.prefix !` while superowner is in server.
  Expected: denied.
- Try `.prefix !` while superowner is not in server.
  Expected: allowed.

## Admin User Tests

Expected: can use admin-power commands, cannot bypass disabled commands, cannot act on server owner or superowner.

### Public Commands

Run the same public commands from Superowner Tests, except `.fsleep` should only work if internally allowed.

Expected: public commands work.

### Admin Commands

Run:

- `.disable calc`
- `.enable calc`
- `.dclist`
- `.rolesinfo`
- `.roleinfo @role`
- `.test`
- `.endttt`
- `.purge 1` and `.purge @normal 1`
- `.rpurge 1` and `.rpurge @normal 1`
- `.steal`
- `.giveaway 1m test prize`
- `.giveaway test prize 1m`
- `.giveaway` then use the setup UI.
- `.listbans`
- `.listblocks`
- `.lists`
- `.censor badtestphrase`
- `.uncensor badtestphrase`
- `.clearcensors`
- `.setlogs`
- `.lock`
- `.unlock`
- `.editactivity channel #channel`
- `.editactivity next 12h`
- `.endactivity`
- `.stopactivity`

Run only if safe:

- `.setnick @normal TestName`
- `.kick @normal test`
- `.ban @normal test`
- `.unban <normal_user_id>`
- `.addrole @normal @role` and `.addrole @role @normal`
- `.removerole @normal @role` and `.removerole @role @normal`
- `.deleterole @role`
- `.lockdown`
- `.reopen`
- `.rlockdown`
- `.runlock`
- `.shut @normal`
- `.unshut @normal`
- `.rshut @normal`
- `.unrshut @normal`
- `.clearwatchlist`
- `.send #channel test message`
- `.reply <message_id_or_link> test reply`
- `.aban @normal`
- `.raban @normal`
- `.abanlist`
- `.summon @normal`
- `.summon2 @normal`
- `.block @normal`
- `.unblock @normal`

Expected: admin can act on normal users, but not server owner or superowner.

### 𝚀𝚞𝚎wo Admin

Run:

- `.editlottery price 200000`
- `.editlottery duration 12h`
- `.editlottery cut 10`
- `.editlottery channel #channel`
- `.stoplottery`
- `.add @normal 1000` and `.add 1000 @normal`
- `.remove @normal 100` and `.remove 100 @normal`
- `.addtick @normal 2` and `.addtick 2 @normal`
- `.settick @normal 5` and `.settick 5 @normal`
- `.setquesos @normal 1000` and `.setquesos 1000 @normal`

Expected: works for admin.

### Admin Permission Checks

- Disable `.calc`, then run `.calc 1+1`.
  Expected: denied while disabled.
- Try admin commands against server owner and superowner.
  Expected: denied.
- Try admin commands against normal user.
  Expected: allowed where Discord hierarchy allows it.
- Try `.prefix !` while superowner is in server.
  Expected: denied.
- Try `.prefix !` while superowner is not in server.
  Expected: allowed.

## Normal User Tests

Expected: public commands work. Admin, activity admin, lottery admin, and 𝚀𝚞𝚎wo admin commands are denied.

### Public Commands

Run:

- `.help`
- `.userinfo`
- `.userinfo @admin`
- `.pfp`
- `.pfp @admin`
- `.q`
- `.dsnipe`
- `.esnipe`
- `.rsnipe`
- `.poll test question? one two`
- `.poll yes no test? yes no 1m`
- `.poll test question | one | two`
- `.poll` then use the setup UI.
- `.picker apple banana orange`
- `.picker "ice cream" pizza sushi`
- `.picker` then use the setup UI.
- `.timer 1m test`
- `.timer test 1m`
- `.timer` then use the setup UI.
- `.ctimer`
- `.alarm 1m test alarm`
- `.alarm test alarm 1m`
- `.alarm` then use the setup UI.
- `.calc 2+2`
- `.calc` then use the setup UI.
- `.define test`
- `.define` then use the setup UI.
- `.afk testing`
- `.sleep`
- `.wake @normal`
- `.setbday 01/01`
- `.setbday` then use the setup UI.
- `.removebday`
- `.activity`
- `.activitystats`
- `.away`
- `.find <user_id>`
- `.ask hello`
- `.generate short test prompt`
- Reply to an image with `.analyse`
- Reply to an image with `.analyze`
- `.translate hello to Italian`

Expected: all public commands run if the required external API/config is available.

### Games

Run:

- `.ttt @other_user`
- `.ttt @other_user` with a bet
- `.c4 @other_user`
- `.c4 @other_user` with a bet
- `.chess @other_user`
- `.chess @other_user` with a bet
- `.move e2e4` or `.chessmove e2e4` during chess fallback testing
- `.resign`

Expected: games work, bet accept prompts appear, result receipts use consistent fields (`Risk`, `Bet`, `Result`, `Prize/Lost`, `Streak`, `New Balance`), daily loss warning appears near 70%, and gambling is blocked before daily losses can exceed 85%.

### 𝚀𝚞𝚎wo

Run:

- `.bal`
- `.cash`
- `.bank`
- `.bank deposit 10k`
- `.bank withdraw 5k`
- `.bank interest`
- `.profile`
- `.level`
- `.lvl`
- `.guide`
- `.streaks`
- `.inventory`
- `.inv`
- `.items`
- `.quests`
- `.dailychallenge`
- `.shop`
- `.tutorial`
- `.tutorial off`
- `.tutorial on`
- `.cooldowns`
- `.transactions`
- `.daily`
- `.weekly`
- `.monthly`
- `.cf 1000 h`
- `.roulette 1000 red`
- `.slots 1000`
- `.blackjack 1000`
- `.scratch 1000`
- `.tower 1000`
- `.vault 1000`
- `.memory 1000`
- `.cardladder 1000`
- `.lockpick 1000`
- `.heist 1000`
- `.diceduel 1000`
- `.cases 1000`
- `.plinko 1000`
- `.luckynumber 1000`
- `.jackpotspin 1000`
- `.dungeon`
- `.ms 1000`
- `.wheel 1000`
- `.gamestats`
- `.achievements`
- `.setbadge`
- `.setbadge clear`
- `.gamebalance`
- `.gamehistory`
- Press Replay in `.gamehistory`.
- `.seasonpass`
- `.recommendgame`
- `.limits`
- `.flagstats`
- `.give @other_user 1000` and `.give 1000 @other_user`
- `.lottery`
- `.lotterystats`
- `.buytick 1`
- `.rob @other_user` while robbing is off.
- After an admin enables robbing, try `.rob @other_user`.
- `.lb`
- `.econhelp`
- `.quewohelp`
- `.explain shop`

Expected: public 𝚀𝚞𝚎wo commands work, balances update correctly, cooldowns apply, tutorial tips can be ended, bank cash cannot be robbed, and lottery status shows personal entries/max-buy/win chance.

### Denial Checks

Run:

- `.disable calc`
- `.setlogs`
- `.purge 1` and `.purge @normal 1`
- `.kick @other_user`
- `.ban @other_user`
- `.addrole @other_user @role` and `.addrole @role @other_user`
- `.setbdaychannel #channel`
- `.setbdaychannel <channel id>`
- `.activity setup`
- `.editactivity channel #channel`
- `.endactivity`
- `.stopactivity`
- `.editlottery price 200000`
- `.stoplottery`
- `.robsettings on`
- `.add @normal 1000` and `.add 1000 @normal`
- `.remove @normal 100` and `.remove 100 @normal`
- `.addtick @normal 1` and `.addtick 1 @normal`
- `.settick @normal 1` and `.settick 1 @normal`
- `.setquesos @normal 1000` and `.setquesos 1000 @normal`
- `.fsleep @other_user 1m`

Expected: denied, unless this normal user is the person who added the bot for birthday/activity setup.

## Shared Feature Tests

Run these after role tests because they need multiple users, waiting, or restart behavior.

### 𝚀𝚞𝚎wo System

- New user balance row creation.
- Balance formatting with Qoins custom emoji.
- Shop buy flow: buy item, hit item limit, insufficient funds.
- Streak Freeze purchase: item appears in inventory and can protect one missed claim streak.
- Fortune Vial purchase: temporary luck boost appears with a live timestamp and expires.
- Tutorial mode: new users get starter tips, the End Tutorial button appears after a few tips, and `.tutorial off/on` works.
- Bank: deposits/withdrawals update cash/bank totals, `.bank interest` respects the daily cooldown, and banked money cannot be robbed.
- Robbing: disabled by default, `.robsettings on/off` works server-wise, failed robs fine the robber, successful robs only touch cash.
- Shop message updates after purchases and refreshes active effect state while open.
- Inventory/profile display after buying items.
- Quest claim and refresh buttons.
- Daily/weekly/monthly cooldowns and streaks.
- Shared 𝚀𝚞𝚎wo cooldown blocks rapid back-to-back 𝚀𝚞𝚎wo commands for normal users.
- Amount parsing: `4m`, `4.5m`, `4k`, `47k`, `734k`, `1b`.
- Slots: only pays when all 3 reels match; payouts are x2, x3, x4, x5 by symbol.
- Scratch: only pays when all 5 symbols match; QScratchMark pays x10, QSlotJackpot pays x12, and win chance is low.
- Tower: safe doors climb multipliers; cash out pays, trapped door resets the universal gambling streak.
- Vault: 3-digit guesses show exact/close hints; correct code pays, failed tries reset the universal gambling streak.
- Memory: matching all 8 pairs pays; too many misses or timeout resets the universal gambling streak.
- Dungeon: free solo run shows room choices, HP, keys, relics, monster duel mini-game, trap wire-sequence mini-game, locked-gate lockpick mini-game, clear reward, timeout, and game stats.
- Roulette: matching color pays x3 and loading display matches result.
- Mine Sweep: multiplier starts at x2.
- Leaderboard: local/global switch, ranking type menu, pages, caller rank, user mentions open profiles.
- Transactions pagination/limits.
- Game history Replay button shows a compact timeline of stored results.
- Season pass page shows monthly goals and points users toward season/achievement progression.
- Recommend Game suggests a bankroll-appropriate next game.
- Chat XP background award and level-up embed.
- Admin/superowner amount override behavior.

### Lottery

- First setup asks for channel and period.
- Lottery channel permissions are applied.
- Panel restores after restart.
- `.lottery` status/check messages include ticket UI and stay synced after restart.
- Lottery status button shows the user's own entries, win chance, round spend, and max additional tickets.
- Buy 10/20/30/custom buttons update pot and panel.
- Buying tickets cannot push a user above 60% lottery spend for the current round; earning or spending quesos changes how many more tickets they can buy.
- Private ticket confirmation works.
- Ticket role assignment works.
- `.lotterystats` pagination works.
- Minimum-player refund path returns money and sends confirmation.
- Winner path pays pot, clears lottery channel, posts winner message, then posts fresh lottery panel.
- `.editlottery price/duration/cut/channel` updates panel.
- `.stoplottery` clears config/tickets.
- `.addtick`, `.settick`, `.setquesos` work with user, role, and everyone in either target/value order.

### Activity Reports

- `.activity setup` dropdown works.
- `.activity setup` channel ID/mention reply works.
- `.activity` shows status if already configured.
- `.activitystats` shows the same status embed.
- Activity setup posts a live activity panel and the bot edits that same panel as message counts change.
- `.editactivity channel #channel` moves reports and keeps next report time.
- `.editactivity next 12h` resets next report timer.
- `.endactivity` clears the report channel, posts previous winners, posts a fresh live activity panel, and keeps reports enabled.
- `.stopactivity` disables reports and clears current window.
- 24-hour report clears the report channel, posts top 5 by messages with custom number emojis, then posts a fresh live activity panel.
- After a report is due, `.activity` shows a fresh next report time even if the report channel was missing or the send failed.
- Activity report loop survives restart.

### Games
- TTT challenge accept/decline/timeout.
- TTT bet proposer prompt and opponent bet accept/decline.
- TTT win/draw/timeout payout.
- TTT custom X/O emojis render.
- C4 challenge accept/decline/timeout.
- C4 bet proposer prompt and opponent bet accept/decline.
- C4 board column numbers show below the grid.
- C4 win/draw/timeout payout.
- C4 does not freeze after repeated moves or full columns.
- Chess challenge accept/decline.
- Chess bet proposer prompt and opponent bet accept/decline.
- Chess UI only lets current player move.
- Chess UI source menu lists movable pieces and legal destinations.
- Chess asks for move confirmation.
- Chess live 10-minute clocks update and award timeout wins.
- Chess board flips to current player's perspective.
- Chess board uses custom number labels and file-letter labels.
- Chess rejects illegal fallback notation.
- Chess detects checkmate, stalemate/draw, resignation, and settles bets.
- Flag Quiz lets the starter choose Solo/Public and 10/20/50/all 197 flags, sends a fresh large flag image prompt per flag with no country flag emoji, gives 30 seconds per guess, edits the same prompt after wrong guesses and offers a Hint button, gives each user 2 tries per flag, accepts small typos, supports starter `skip`/`stop`, tracks public scores per user, and pays 20,000 quesos per correct flag.
- Dungeon is solo-only, rejects other users pressing its buttons, and records clear/fail stats.

### Timers, Polls, Giveaway

- Timer create, countdown edit, finish ping, restart restore.
- Timer cancel UI, superowner cancel override.
- Alarm set and final ping.
- Poll yes/no and multi-option creation.
- Poll custom number emojis render.
- Poll reaction updates.
- Poll timed end and manual `.epoll`.
- Poll restore after restart.
- Giveaway winner path and no-entry path.

### Status Features

- AFK set, mention response, welcome-back summary.
- Sleep set, mention response, wake-up summary.
- Forced sleep/wake permission behavior.
- Birthday set/remove.
- Birthday channel setup.
- Midnight birthday announcement posts in every configured server where the birthday user is still a member.
- `.away` output.

### Moderation / Server Tools

- Message delete/edit logs and snipes.
- Purge/rpurge and attachment logging.
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
