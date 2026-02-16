-- Demo seed data for local/dev.
-- Note: Only applied on first init of a fresh Postgres volume.

INSERT INTO tenants (id, name, slug)
VALUES ('11111111-1111-1111-1111-111111111111', 'Demo Tenant', 'demo')
ON CONFLICT (slug) DO NOTHING;

INSERT INTO projects (id, tenant_id, name)
VALUES (
  '22222222-2222-2222-2222-222222222222',
  '11111111-1111-1111-1111-111111111111',
  'Demo Project'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO models (id, key, name, category)
VALUES ('33333333-3333-3333-3333-333333333333', 'd8-fast', 'D8 Fast', 'hydrology')
ON CONFLICT (key) DO NOTHING;

