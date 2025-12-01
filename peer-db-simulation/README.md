# code-with-quarkus

=====================================================================================
SINK DATABASE
=====================================================================================
CREATE TABLE public.migration_status (
	table_name varchar(255) NOT NULL,
	batch_id varchar(255) NOT NULL,
	records_migrated int4 NOT NULL,
	migration_timestamp timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	batch_start_time timestamp NULL,
	batch_end_time timestamp NULL,
	status varchar(50) DEFAULT 'COMPLETED'::character varying NULL,
	error_message text NULL,
	CONSTRAINT migration_status_pkey PRIMARY KEY (table_name, batch_id)
);
CREATE INDEX idx_migration_status_table ON public.migration_status USING btree (table_name, migration_timestamp DESC);
CREATE INDEX idx_migration_status_timestamp ON public.migration_status USING btree (migration_timestamp DESC);

CREATE TABLE public.orders (
	id int8 NOT NULL,
	synced_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	"_replication_batch_id" varchar(50) NULL,
	amount numeric(15, 2) NULL,
	user_id int8 NULL,
	status varchar(50) NULL,
	created_at timestamptz NULL,
	order_id int8 NULL,
	CONSTRAINT orders_pkey PRIMARY KEY (id)
);

CREATE TABLE public.products (
	id int8 NOT NULL,
	synced_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	"_replication_batch_id" varchar(50) NULL,
	price numeric(15, 2) NULL,
	"name" varchar(255) NULL,
	category varchar(100) NULL,
	stock_quantity int4 NULL,
	last_modified timestamptz NULL,
	product_id int8 NULL,
	CONSTRAINT products_pkey PRIMARY KEY (id)
);

CREATE TABLE public.replication_state (
	table_name varchar(255) NOT NULL,
	last_sync_timestamp timestamp NULL,
	last_sync_id varchar(255) NULL,
	rows_processed int8 DEFAULT 0 NULL,
	last_updated timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT replication_state_pkey PRIMARY KEY (table_name)
);

CREATE TABLE public.users (
	id int8 NOT NULL,
	synced_at timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	"_replication_batch_id" varchar(50) NULL,
	updated_at timestamptz NULL,
	created_at timestamptz NULL,
	"name" varchar(255) NULL,
	email varchar(255) NULL,
	CONSTRAINT users_pkey PRIMARY KEY (id)
);


=====================================================================================
SOURCE DATABASE
=====================================================================================
CREATE TABLE public.order_items (
	order_item_id bigserial NOT NULL,
	order_id int8 NOT NULL,
	product_id int8 NOT NULL,
	quantity int4 NOT NULL,
	unit_price numeric(15, 2) NOT NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT order_items_pkey PRIMARY KEY (order_item_id),
	CONSTRAINT order_items_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.orders(order_id),
	CONSTRAINT order_items_product_id_fkey FOREIGN KEY (product_id) REFERENCES public.products(product_id)
);
CREATE INDEX idx_order_items_created_at ON public.order_items USING btree (created_at);
CREATE INDEX idx_order_items_order_id ON public.order_items USING btree (order_id);

CREATE TABLE public.orders (
	order_id bigserial NOT NULL,
	user_id int8 NOT NULL,
	amount numeric(15, 2) NOT NULL,
	status varchar(50) DEFAULT 'pending'::character varying NOT NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT orders_pkey PRIMARY KEY (order_id),
	CONSTRAINT orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(id)
);
CREATE INDEX idx_orders_created_at ON public.orders USING btree (created_at);
CREATE INDEX idx_orders_updated_at ON public.orders USING btree (updated_at);
CREATE INDEX idx_orders_user_id ON public.orders USING btree (user_id);

CREATE TABLE public.products (
	product_id bigserial NOT NULL,
	"name" varchar(255) NOT NULL,
	description text NULL,
	price numeric(15, 2) NOT NULL,
	category varchar(100) NULL,
	stock_quantity int4 DEFAULT 0 NOT NULL,
	last_modified timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT products_pkey PRIMARY KEY (product_id)
);
CREATE INDEX idx_products_category ON public.products USING btree (category);
CREATE INDEX idx_products_last_modified ON public.products USING btree (last_modified);

CREATE TABLE public.users (
	id bigserial NOT NULL,
	"name" varchar(255) NOT NULL,
	email varchar(255) NOT NULL,
	created_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	updated_at timestamptz DEFAULT CURRENT_TIMESTAMP NULL,
	CONSTRAINT users_email_key UNIQUE (email),
	CONSTRAINT users_pkey PRIMARY KEY (id)
);
CREATE INDEX idx_users_created_at ON public.users USING btree (created_at);
CREATE INDEX idx_users_updated_at ON public.users USING btree (updated_at);