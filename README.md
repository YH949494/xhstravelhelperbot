# xhstravelhelperbot

## Skills & Memory Files

Edit skill and memory markdown files in `skills/*.md`.

- `growth_rules.md`: core growth constraints and operating rules.
- `script_framework.md`: reusable script structure guidance.
- `hook_library.md`: high-performing hook patterns.
- `series_registry.md`: recurring content series notes.
- `performance_log.md`: successful patterns and win records.
- `failure_log.md`: rejected hooks/titles and failure notes.

Skill loading is deterministic: rules/framework/hook files load before series and logs, and logs are injected as reference context after rules.

## Learn Script Command

`/learn_script` only works in Telegram groups/supergroups.

Optional: set `ALLOWED_GROUP_CHAT_IDS` (comma-separated group chat IDs) to restrict which groups can use it.

Usage:

```
/learn_script
platform: xhs
type: cost_breakdown
performance: 3.2k saves / 210 comments

[full script content...]
```

The bot analyzes the script via OpenAI and appends extracted hook/rules into `skills/*.md` without storing the full script text.
