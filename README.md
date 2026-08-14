# Investment System

用于投资决策的长期规则库。

## 文件结构

- `.agents/skills/investment-rules/SKILL.md`
  - 通用投资原则与调用流程。
- `.agents/skills/investment-rules/references/A_SHARE.md`
  - A股特有规则。
- `.agents/skills/investment-rules/references/US_HK.md`
  - 美股、港股及期权特有规则。
- 私有状态库中的 `STATE.md`
  - 记录具体标的的仓位、估值、买点、加仓条件及失效条件。
  - 属于动态状态，不属于长期规则。

## 使用原则

1. 先应用 `SKILL.md` 的通用规则。
2. 再读取对应市场规则。
3. 涉及具体标的、仓位或交易决策时，再读取可用的 `STATE.md`。
4. 临时观点不得自动升级为长期规则。
5. 只有使用者明确确认“升格正式规则”后，才修改长期规则文件。
