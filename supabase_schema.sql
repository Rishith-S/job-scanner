-- Job monitor shared DB: run once in the Supabase SQL editor.
-- Two tables mirroring the SQLite honesty contract. Writes come only from
-- GitHub Actions with the service_role key; the site reads with the anon key.

create table if not exists companies (
  company text primary key,
  ats text not null default '',
  scan_status text not null default '',
  detail text not null default '',
  portal text not null default '',
  slice text not null default 'all',
  half text not null default 'all',
  last_completed_at timestamptz,
  updated_at timestamptz not null default now()
);

create table if not exists jobs (
  job_key text primary key,
  company text not null references companies(company) on delete cascade,
  title text not null default '',
  location text not null default '',
  url text not null default '',
  entry_evidence text not null default '',
  profile_fit text not null default '',
  first_seen_at timestamptz,
  last_seen_at timestamptz,
  updated_at timestamptz not null default now()
);

create index if not exists jobs_company_idx on jobs(company);
create index if not exists jobs_first_seen_idx on jobs(first_seen_at desc);

alter table companies enable row level security;
alter table jobs enable row level security;

-- Public read for the site. No write policies for anon: inserts, updates
-- and deletes only accept the service_role key (Actions secrets).
drop policy if exists "public read" on companies;
create policy "public read" on companies for select to anon using (true);

drop policy if exists "public read" on jobs;
create policy "public read" on jobs for select to anon using (true);
