CREATE TABLE IF NOT EXISTS users (
	id TEXT PRIMARY KEY,
	email TEXT,
	password_hash TEXT,
	tier TEXT,
	created_at DATETIME
);
CREATE TABLE IF NOT EXISTS projects (
	id TEXT PRIMARY KEY,
	owner_id TEXT,
	team_id TEXT,
	name TEXT,
	framework TEXT,
	created_at DATETIME
);
CREATE TABLE IF NOT EXISTS teams (
	id TEXT PRIMARY KEY,
	name TEXT,
	created_at DATETIME
);
CREATE TABLE IF NOT EXISTS team_members (
	team_id TEXT,
	user_id TEXT,
	role TEXT,
	joined_at DATETIME,
	PRIMARY KEY (team_id, user_id)
);
CREATE TABLE IF NOT EXISTS billing_subscriptions (
	id TEXT PRIMARY KEY,
	user_id TEXT,
	team_id TEXT,
	plan TEXT,
	status TEXT,
	stripe_customer_id TEXT,
	created_at DATETIME,
	updated_at DATETIME
);
CREATE TABLE IF NOT EXISTS deployments (
	id TEXT PRIMARY KEY,
	project_id TEXT,
	status TEXT,
	image_name TEXT,
	container_id TEXT,
	public_url TEXT,
	internal_url TEXT,
	tunnel_id TEXT,
	created_at DATETIME
);
CREATE TABLE IF NOT EXISTS domains (
	id TEXT PRIMARY KEY,
	project_id TEXT,
	hostname TEXT,
	ssl_enabled BOOLEAN
);
CREATE TABLE IF NOT EXISTS environment_variables (
	id TEXT PRIMARY KEY,
	project_id TEXT,
	key TEXT,
	value TEXT
);
CREATE TABLE IF NOT EXISTS build_logs (
	id TEXT PRIMARY KEY,
	deployment_id TEXT,
	timestamp DATETIME,
	message TEXT
);

CREATE TABLE IF NOT EXISTS logs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	deployment_id TEXT NOT NULL,
	log_type TEXT NOT NULL,
	level TEXT NOT NULL DEFAULT 'info',
	message TEXT NOT NULL,
	timestamp DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_logs_deployment_id ON logs(deployment_id);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);

CREATE TABLE IF NOT EXISTS env_vars (
	id TEXT PRIMARY KEY,
	project_id TEXT NOT NULL,
	key TEXT NOT NULL,
	value TEXT NOT NULL,
	created_at DATETIME,
	UNIQUE(project_id, key)
);

CREATE TABLE IF NOT EXISTS payment_methods (
	id TEXT PRIMARY KEY,
	context_id TEXT,
	brand TEXT,
	last4 TEXT,
	exp TEXT,
	is_default BOOLEAN,
	created_at DATETIME
);

CREATE TABLE IF NOT EXISTS invoices (
	id TEXT PRIMARY KEY,
	context_id TEXT,
	amount REAL,
	status TEXT,
	date DATETIME
);

CREATE TABLE IF NOT EXISTS databases (
	name TEXT PRIMARY KEY,
	status TEXT,
	ports TEXT
);
