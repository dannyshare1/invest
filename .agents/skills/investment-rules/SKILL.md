---
name: investment-rules
description: Apply the user's durable investment rules before giving A-share, US/HK equity, ETF, portfolio, valuation, Sell Put, Covered Call, Wheel, earnings, buy/sell, or risk-management decisions.
---

# Investment Rules

This skill is the executable entrypoint for the user's durable investment framework.

## Load order

Always read:

1. `references/CORE.md`
2. The relevant market file:
   - `references/A_SHARE.md`
   - `references/US_HK.md`
3. If available and relevant, repository-root dynamic files:
   - `WATCHLIST.md`
   - `TASKS.md`
   - `CURRENT_VIEWS.md`

## Core workflow

1. Identify market, security type, portfolio context, and whether options are involved.
2. Fetch current, verifiable data before relying on an old conclusion.
3. Separate facts from judgments.
4. Recalculate valuation using current price and current earnings expectations when reliable data are available.
5. Check cash, concentration, assignment exposure, and portfolio impact before recommending an action.
6. Use technical levels only as secondary timing evidence.
7. Give only a small number of executable actions, each with a price/valuation/event trigger and, when relevant, suggested size.
8. If the conclusion is unchanged, explain why current data still support it.

## Rule update protocol

When the user says `升格正式规则`, `加入正式规则`, or clearly asks to make a principle durable:

1. Decide whether it belongs in `CORE.md`, `A_SHARE.md`, or `US_HK.md`.
2. Generalize it into a durable rule; do not preserve a one-day market opinion as policy.
3. Update the relevant file.
4. Add a dated entry to `references/CHANGELOG.md`.
5. Check whether existing scheduled investment tasks contain conflicting embedded rules; update them when task-editing tools are available.

## Dynamic-state protocol

- A security being watched for a future trigger belongs in `WATCHLIST.md`.
- A concrete unresolved future action belongs in `TASKS.md`.
- A temporary market or company view belongs in `CURRENT_VIEWS.md`.
- Never promote dynamic state into permanent rules unless the user explicitly asks.

## Data discipline

Never invent real-time prices, analyst consensus, account positions, option Greeks, or corporate events. Clearly label delayed or stale data. Prefer current account data, company filings/releases, exchange data, reputable market-data sources, and current consensus data when available.
