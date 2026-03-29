/*
  Model  : fact_sale_order
  Schema : facts
  Grain  : One row per Odoo sale order (sale.order)

  Business logic:
    - Joins sale orders with their linked CRM opportunity to pull project type
      and payment method (stored on the lead, not the order).
    - Derives the last state-change date from mail_tracking_value so Power BI
      can calculate days-in-status without hitting Odoo audit tables.
    - Filters to orders created in 2025+ OR orders with a recent state update
      (i.e., old deals that moved in the current reporting period).
    - All FK columns default to '0' (sentinel row in every dimension) so
      Power BI never has unmatched keys.

  Indexes added via post-hook (see dbt_project.yml):
    idx_fact_sale_order_create_date, _vendor, _client, _state
*/

{{ config(
    materialized = 'table',
    schema       = 'facts',
    indexes      = [{'columns': ['id'], 'unique': true}],
    tags         = ['fact', 'commercial']
) }}

-- ────────────────────────────────────────────────────────────
-- Step 1: Derive date of last state change from audit trail
--         mail_tracking_value ∩ mail_message ∩ ir_model_fields
-- ────────────────────────────────────────────────────────────
with last_state_change_per_order as (

    select
        cast(mail_msg.res_id as integer)           as sale_order_id,
        to_char(max(tracking.create_date), 'YYYYMMDD') as date_last_state_change_key

    from {{ ref('stg_mail_tracking_value') }}  tracking
    join {{ ref('stg_mail_message') }}         mail_msg
        on tracking.mail_message_id = mail_msg.id
    join {{ ref('stg_ir_model_fields') }}      model_field
        on tracking.field_id = model_field.id

    where mail_msg.model  = 'sale.order'
      and model_field.name = 'state'    -- track only status transitions

    group by mail_msg.res_id

),

-- ────────────────────────────────────────────────────────────
-- Step 2: Enrich sale orders with linked CRM opportunity data
--         (project type and payment method live on the lead)
-- ────────────────────────────────────────────────────────────
sale_orders_with_crm_context as (

    select
        so.id                                          as order_id,
        so.name                                        as order_reference,
        so.company_id,
        so.partner_id                                  as client_id,
        so.user_id                                     as vendor_id,
        so.team_id,
        so.state,
        so.lost_reason_id,
        so.opportunity_id,
        so.rbe_status,

        -- Financial measures
        so.amount_untaxed,
        so.amount_tax,
        so.amount_total,
        so.amount_to_invoice,

        -- Instalment measures (French "acomptes")
        so.acompte_1_pourcent                          as instalment_1_pct,
        so.acompte_1                                   as instalment_1_amount,
        so.acompte_1_bis                               as instalment_1_bis_amount,
        so.acompte_2_pourcent                          as instalment_2_pct,
        so.acompte_2                                   as instalment_2_amount,
        so.acompte_3_pourcent                          as instalment_3_pct,
        so.acompte_3                                   as instalment_3_amount,

        -- Date keys (YYYYMMDD — join to dim_date.id)
        so.date_create_id                              as date_order_created_key,
        so.date_order_id                               as date_order_confirmed_key,
        so.date_signed_id                              as date_contract_signed_key,
        so.date_validity_id                            as date_quote_expiry_key,
        so.date_statut_signe_id                        as date_status_signed_key,

        -- CRM context (may be null if no linked opportunity)
        coalesce(crm.project_type_id, '0')             as project_type_id,
        coalesce(crm.payment_mode, '0')                as payment_method_id,
        coalesce(
            to_char(crm.x_studio_date_rbe, 'YYYYMMDD'),
            '0'
        )                                              as date_rbe_key

    from {{ ref('stg_sale_order') }}   so
    left join {{ ref('stg_crm_lead') }} crm
        on so.opportunity_id = crm.id
        and so.opportunity_id != '0'

),

-- ────────────────────────────────────────────────────────────
-- Step 3: Attach last state change date and resolve dimension keys
-- ────────────────────────────────────────────────────────────
enriched_orders as (

    select
        so.*,
        coalesce(audit.date_last_state_change_key, '0') as date_last_state_change_key,

        -- Resolve sale status dimension key
        coalesce(dim_status.id, '0')                    as sale_status_id,

        -- Resolve RBE status dimension key (case-insensitive match)
        coalesce(dim_rbe.id, '0')                       as rbe_status_id

    from sale_orders_with_crm_context              so
    left join last_state_change_per_order          audit
        on cast(so.order_id as integer) = audit.sale_order_id

    left join {{ ref('dim_sale_status') }}         dim_status
        on lower(so.state) = dim_status.state

    left join {{ ref('dim_rbe_status') }}          dim_rbe
        on lower(trim(so.rbe_status)) = lower(trim(dim_rbe.label))

),

-- ────────────────────────────────────────────────────────────
-- Step 4: Final projection — apply reporting period filter
-- ────────────────────────────────────────────────────────────
final as (

    select
        order_id                      as id,
        order_reference               as order_name,

        -- Foreign keys to dimensions (never null — sentinel '0' used)
        vendor_id,
        client_id,
        company_id,
        team_id,
        sale_status_id                as id_state,
        project_type_id,
        payment_method_id,
        rbe_status_id,
        lost_reason_id,

        -- Date dimension keys
        date_order_created_key        as date_create_id,
        date_order_confirmed_key      as date_order_id,
        date_contract_signed_key      as date_signed_id,
        date_quote_expiry_key         as date_validity_id,
        date_status_signed_key        as date_statut_signe_id,
        date_rbe_key                  as date_rbe_id,
        date_last_state_change_key    as date_last_state_update_id,

        -- Measures
        amount_untaxed,
        amount_tax,
        amount_total,
        amount_to_invoice,
        instalment_1_pct              as acompte_1_pourcent,
        instalment_1_amount           as acompte_1,
        instalment_1_bis_amount       as acompte_1_bis,
        instalment_2_pct              as acompte_2_pourcent,
        instalment_2_amount           as acompte_2,
        instalment_3_pct              as acompte_3_pourcent,
        instalment_3_amount           as acompte_3

    from enriched_orders

    where
        -- Include 2025+ orders
        left(date_order_created_key, 4) >= '2025'
        -- OR older orders that had a state change in the current reporting period
        or (
            left(date_order_created_key, 4) < '2025'
            and date_last_state_change_key >= '20250101'
        )

)

select * from final