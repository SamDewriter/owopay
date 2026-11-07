-- OwoPay's OLTP tables, in the owopay database. docs/schema.md describes every column.

create table if not exists customers (
    customer_id     text primary key,
    full_name       text not null,
    phone_number    text not null,
    bvn             text not null,
    state           text not null,
    kyc_tier        smallint not null check (kyc_tier between 1 and 3),
    signup_channel  text not null,
    created_at      timestamptz not null,
    updated_at      timestamptz not null
);

create table if not exists merchants (
    merchant_id     text primary key,
    business_name   text not null,
    category        text not null,
    state           text not null,
    created_at      timestamptz not null
);

create table if not exists agents (
    agent_id        text primary key,
    business_name   text not null,
    state           text not null,
    created_at      timestamptz not null
);

create table if not exists wallets (
    wallet_id       text primary key,
    customer_id     text not null unique references customers,
    balance_kobo    bigint not null,
    currency        char(3) not null default 'NGN',
    updated_at      timestamptz not null
);

create table if not exists transactions (
    txn_id          uuid primary key,
    customer_id     text not null references customers,
    merchant_id     text references merchants,
    agent_id        text references agents,
    channel         text not null,
    amount_kobo     bigint not null check (amount_kobo > 0),
    status          text not null,
    created_at      timestamptz not null
);

create index if not exists transactions_created_at_idx on transactions (created_at);
create index if not exists transactions_customer_id_idx on transactions (customer_id);
