# Investment System

这个仓库是投资决策的唯一正式规则源（source of truth）。

## 结构

- `.agents/skills/investment-rules/SKILL.md`：Codex/Agent 调用入口与工作流程。
- `.agents/skills/investment-rules/references/CORE.md`：所有市场通用规则。
- `.agents/skills/investment-rules/references/A_SHARE.md`：A股规则。
- `.agents/skills/investment-rules/references/US_HK.md`：美股、港股与期权规则。
- `.agents/skills/investment-rules/references/CHANGELOG.md`：正式规则变更记录。
- `WATCHLIST.md`：当前重点观察标的与触发条件。
- `TASKS.md`：未完成的投资检查、财报复核、买点/止盈/期权处理事项。
- `CURRENT_VIEWS.md`：当前阶段性观点，不属于长期规则。

## 使用原则

1. 做任何投资决策前，先读取最新规则。
2. 临时观点不能直接写进长期规则。
3. 用户明确说“升格正式规则”时，才更新长期规则并写入 CHANGELOG。
4. 买点、财报后复核、期权到期处理等未来事项写入 WATCHLIST / TASKS。
5. 规则修改后，应同步检查现有投资定时任务是否存在冲突。

## Work 使用方式

如果当前环境不能原生调用 Skill，则把本仓库当作规则库：做投资答疑前先读取 `CORE.md`，再根据市场读取 `A_SHARE.md` 或 `US_HK.md`，然后读取 `WATCHLIST.md`、`TASKS.md`、`CURRENT_VIEWS.md` 中与当前问题有关的内容。

## Codex 使用方式

在支持 Skill 的环境中调用：

`$investment-rules`

然后再提出具体投资问题。
