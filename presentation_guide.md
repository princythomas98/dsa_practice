I'll walk you through our CDC pipeline where i implemented SCD Type 2 implementation that transformed our data architecture from basic reporting to predictive analytics with complete audit trails

Business objective was - that our risk team should be able to analyse credit scores at the exact moment each loan was approved — not the current value, but the historical value at approval time. Separating good loans from NPAs on this basis is what drove the default rate from 4.2% down to 3.1%.


Before diving into the architecture, here is what this pipeline delivered — in numbers.
Loan default rate → 4.2% → 3.1%
Temporal join query time → 45 min → 6 min
Storage cost reduction (selective tracking) → 70% savings
Change-detection speed (hash vs column comparison) → 60% faster


2. SOURCE LAYER  -[ POSTGRES DB ,15 core tables , customers, loans, and repayments]

"We start with PostgreSQL as our OLTP system—our production database handling 5 million active loans across 15 core tables including customers, loans, and repayments.
The business problem was clear: when data changed—like a customer's credit score going from 720 to 750—the old value was overwritten. Our risk team couldn't answer critical questions like 'what was the customer's credit score when we approved this loan three months ago?'
We also had compliance issues. RBI requires complete audit trails, and running analytics directly on production Postgres was taking 20-30 minutes, impacting our loan approval process."


3. CDC LAYER -[WAL STREAM, DEBEIUM KAFKA CONNECT , READ WAL , ZERO PROD IMPACT]


"For CDC, we deployed Debezium as a Kafka Connect connector. 
Debezium reads the PostgreSQL Write-Ahead Log— its a transaction journal that records every insert, update, and delete BEFORE they're committed to the database.
The key advantage is zero impact on production. We're reading logs, not querying tables. No overhead is on our transactional system whatsoever.
Debezium converts these WAL entries into structured JSON events containing:

Operation type (insert/update/delete)
Timestamp
Before values (what changed FROM)
After values (what changed TO)

These events get published to Kafka topics in real-time."

point to emphasize: "This is log-based CDC, not trigger-based or query-based. It's the most performant approach."


4. MESSAGE QUEUE -[APACHE KAFKA , TOPICS : cdc.public.customers, cdc.public.loans , RETENETION 7 DAYS]

"These CDC events flow into Apache Kafka, which acts as our distributed message queue.
We have separate topics for each table—like cdc.public.customers, cdc.public.loans. This gives us clean separation and independent scalability.
Kafka retains these messages for 7 days, which gives us fault tolerance. If Databricks goes down for maintenance or troubleshooting, we don't lose any changes. When it comes back up, it picks up right where it left off.
This decoupling is critical—source and target systems are completely independent."


5. DATABRICKS - BRONZE LAYER [Structured streamingh , raw cdc events , immutable leanding zone, near real time, 15 min refresh]

"In Databricks, we use Structured Streaming to consume from Kafka in near real-time.
We parse the JSON events and land them in our Bronze layer as raw CDC events in Delta format. This is our immutable landing zone—every single change event is preserved exactly as received.
so If anything goes wrong downstream, we can always replay from here.
The stream runs continuously with a 15-minute refresh interval."

6. DATABRICKS SILVER LAYER - [batch job 15 mins , SCD TYPE2 TABLES ACTIVE RECORDS TABLE , HISTORICAL RECORDS]
Point to: SCD Type 2 tables and batch job
What to say:

"Then a batch job runs every 15 minutes, reading from Bronze and applying our SCD Type 2 transformations to write to the Silver layer.
Now, what is SCD Type 2? Instead of updating records, we preserve complete history.
When a customer's credit score changes from 720 to 750:

We CLOSE the old record: set end_date = today, is_active = false
We INSERT a new record with the updated values: effective_date = today, is_active = true

Both versions are preserved. We can now answer: What was the credit score on any given date?
Critical split: After 6 months, we had 45 million records from 5 million customers. We split into two tables:

Active Records Table: 5M records for fast operational queries (25 seconds)
Historical Records Table: 45M records for audit and analysis"

 "This split reduced query time from 8 minutes to 25 seconds."


7. SCD TYPE 2 TRANSFORMATION LOGIC 

"Let me explain the four key  decisions that make this performant:

A. Surrogate Keys 

"We use auto-generated integer IDs instead of natural customer IDs because  Natural keys can change, and we need stable identifiers when we have multiple versions of the same customer record."

B. Hash-Based Change Detection 

"Instead of comparing 9 columns individually to detect what changed, we concatenate them and generate an MD5 hash.
If the hash changed → something changed. If hash is same → skip processing.
This is 60% faster than column-by-column comparison. At 5 million records, this matters."

C. Selective Tracking 
"We only apply SCD Type 2 to 9 business-critical columns:

credit_score
income
employment_status
address

We DON'T track operational columns like 'last_login' or 'page_views'—that would create millions of useless historical records.
This saved us 70% on storage costs."

D. Record Versioning (Point to blue box)

"Every record has three temporal attributes:

effective_date: When this version became true
end_date: When this version was superseded
is_active: Boolean flag for current version

This enables point-in-time queries and temporal joins."

What to say:

"One of our biggest challenges was temporal joins. We have 100 million payment transactions that need to join with customer dimensions to get attributes AS THEY WERE on the payment date.
The SQL looked like:

sqlWHERE payment_date BETWEEN dim.effective_date AND dim.end_date

This BETWEEN condition was killing us—45 minutes for a single query.

Our solution: Bridge Table
We pre-compute for every date which surrogate key was active. This turns expensive BETWEEN joins into simple equality joins:

sqlWHERE payment_date = bridge.date
AND bridge.surrogate_key = dim.surrogate_key
Query time: 45 minutes down to 6 minutes."

Point to Active/Historical Split:

"And as I mentioned, splitting active and historical tables with proper partitioning by year and Z-ordering on customer_ID brought operational queries from 8 minutes to 25 seconds."


9. DATA QUALITY 
 We implemented Great Expectations with three validation suites that run automatically after every load:

Suite 1: Source-to-Target Reconciliation


Row count matches
No duplicate keys
All source records accounted for


Suite 2: Data Integrity


Credit scores between 300-900
No nulls in mandatory fields
MD5 checksum validation (hash of all active records matches source)


