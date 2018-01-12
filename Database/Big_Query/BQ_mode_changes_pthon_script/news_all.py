def create_table(dataset_id, table_id="news_all", project=None):
    """Creates a simple table in the given dataset.

    If no project is specified, then the currently active project is used.
    """
    bigquery_client = bigquery.Client(project=project)
    dataset_ref = bigquery_client.dataset(dataset_id)

    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref)

    # Set the table schema
    table.schema = (
        bigquery.SchemaField('NEWS_DATE_NewsDim','STRING','REQUIRED'),
        bigquery.SchemaField('score','FLOAT','NULLABLE'),
        bigquery.SchemaField('NEWS_PUBLICATION_NewsDim','STRING','NULLABLE'),
		bigquery.SchemaField('categorised_tag','STRING','REPEATED'),
		bigquery.SchemaField('constituent_id','STRING','REQUIRED'),
		bigquery.SchemaField('NEWS_ARTICLE_TXT_NewsDim','STRING','REQUIRED'),
		bigquery.SchemaField('sentiment','STRING','REQUIRED'),
		bigquery.SchemaField('NEWS_TITLE_NewsDim','STRING','REQUIRED'),
		bigquery.SchemaField('entity_tags','RECORD','NULLABLE')
		bigquery.SchemaField('entity_tags.FACILITY','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.QUANTITY','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.EVENT','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.PERSON','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.DATE','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.TIME','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.CARDINAL','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.PRODUCT','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.LOC','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.WORK_OF_ART','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.GPE','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.PERCENT','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.FAC','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.ORDINAL','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.ORG','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.NORP','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.LANGUAGE','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.MONEY','STRING','REPEATED'),
		bigquery.SchemaField('entity_tags.LAW','STRING','REPEATED'),
		bigquery.SchemaField('constituent_name','STRING','REQUIRED'),
		bigquery.SchemaField('count','INTEGER','NULLABLE'),
		bigquery.SchemaField('url','STRING','NULLABLE'),
		bigquery.SchemaField('news_language','STRING','NULLABLE'),
		bigquery.SchemaField('news_id','INTEGER','NULLABLE'),
		bigquery.SchemaField('news_country','STRING','NULLABLE'),
		bigquery.SchemaField('news_companies','STRING','NULLABLE'),
		bigquery.SchemaField('news_region','STRING','NULLABLE'),
		bigquery.SchemaField('constituent','STRING','REQUIRED'),
		bigquery.SchemaField('To_date','TIMESTAMP','REQUIRED'),
		bigquery.SchemaField('From_date','TIMESTAMP','REQUIRED'),
    )

    table = bigquery_client.create_table(table)

    print('Created table {} in dataset {}.'.format(table_id, dataset_id))