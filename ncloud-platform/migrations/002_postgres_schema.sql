CREATE TYPE deployment_state AS ENUM ('queued', 'building', 'deploying', 'running', 'failed');
CREATE TYPE team_role AS ENUM ('owner', 'admin', 'member');
CREATE TYPE billing_plan AS ENUM ('hobby', 'pro', 'ultra');
CREATE TYPE billing_status AS ENUM ('active', 'past_due', 'canceled', 'trialing');

CREATE TABLE IF NOT EXISTS users (
	id TEXT PRIMARY KEY,
	email TEXT UNIQUE NOT NULL,
	password_hash TEXT NOT NULL,
	tier TEXT,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS teams (
	id TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS team_members (
	team_id TEXT REFERENCES teams(id) ON DELETE CASCADE,
	user_id TEXT REFERENCES users(id) ON DELETE CASCADE,
	role team_role NOT NULL,
	joined_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	PRIMARY KEY (team_id, user_id)
);

CREATE TABLE IF NOT EXISTS billing_subscriptions (
	id TEXT PRIMARY KEY,
	user_id TEXT REFERENCES users(id) ON DELETE CASCADE,
	team_id TEXT REFERENCES teams(id) ON DELETE CASCADE,
	plan billing_plan NOT NULL,
	status billing_status NOT NULL,
	stripe_customer_id TEXT,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	CHECK (
		(user_id IS NOT NULL AND team_id IS NULL) OR 
		(user_id IS NULL AND team_id IS NOT NULL)
	)
);

CREATE TABLE IF NOT EXISTS projects (
	id TEXT PRIMARY KEY,
	owner_id TEXT REFERENCES users(id) ON DELETE SET NULL,
	team_id TEXT REFERENCES teams(id) ON DELETE SET NULL,
	name TEXT NOT NULL,
	framework TEXT,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	CHECK (
		(owner_id IS NOT NULL AND team_id IS NULL) OR 
		(owner_id IS NULL AND team_id IS NOT NULL)
	)
);

CREATE TABLE IF NOT EXISTS deployments (
	id TEXT PRIMARY KEY,
	project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
	status deployment_state NOT NULL DEFAULT 'queued',
	image_name TEXT,
	container_id TEXT,
	public_url TEXT,
	internal_url TEXT,
	tunnel_id TEXT,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS domains (
	id TEXT PRIMARY KEY,
	project_id TEXT REFERENCES projects(id) ON DELETE CASCADE,
	hostname TEXT UNIQUE NOT NULL,
	ssl_enabled BOOLEAN DEFAULT FALSE,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS build_logs (
	id TEXT PRIMARY KEY,
	deployment_id TEXT REFERENCES deployments(id) ON DELETE CASCADE,
	timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
	message TEXT NOT NULL
);
