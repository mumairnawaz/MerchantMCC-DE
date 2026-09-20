# 02 — Business Problem

## Why merchant and institutional data matters in payments

Issuers use merchant-level and MCC data for authorization/risk decisions, dispute/
chargeback merchant matching, and cardholder-facing categorization. Networks standardize
MCC and differentiate interchange by it. Program owners use MCC for rewards-tier design,
spend controls, and merchant-partnership selection. Card-linked-offer platforms match a
live transaction to an offer using MCC and merchant identity as the join key — this is
documented current industry practice, not a FinPay invention.

## The specific gap FinPay addresses

Real, individual card-network transaction data is not publicly available anywhere, at any
price tier, for legitimate free use. A portfolio project in this domain therefore has to do
one of two things: fabricate a "real-looking" API that doesn't exist, or be honest about
building on real reference/institutional data plus clearly labeled synthetic transaction
events. FinPay takes the second path, and treats getting that distinction right as itself
part of the engineering problem.

## Stakeholder needs FinPay's architecture is designed around

- **Issuer / network / program owner**: governed data files with control totals and audit
  metadata, not just a dashboard they can't load into their own systems
- **Internal operations**: reconciliation between authorization, clearing, and settlement
- **Rewards/CLO**: MCC-based offer matching against transaction activity
- **Data engineering operations**: a pipeline that's auditable, incrementally refreshable,
  and validated — not a one-shot script
