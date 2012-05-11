
--
-- Enrich the signals that are defined by start and end timeframe
--

DEFINE ExtractLinks com.dachisgroup.analytics.pig.ExtractLinks();
DEFINE ClassifySentiment com.dachisgroup.analytics.pig.ClassifySentiment();
DEFINE ExtractMentions com.dachisgroup.analytics.pig.ExtractMentions();

SET default_parallel $DEFAULT_REDUCERS;

--
-- Loading the sentiment classifier word list requires more memory than usual
--
SET mapred.child.java.opts -Xmx768m; -- FIXME, this should be a workflow option

--
-- Mapping from dachis account id to twitter screen name pulled from previous wf step
--
tweet_account_all = LOAD 'tweet_account' AS (dachis_account_id:chararray, name:chararray);

--
-- Operate on only signals that have been enriched by 'enrich_signals_with_snapshot_data.pig'
-- These are already only within the appropriate time bucket
--
signals = LOAD 'temporary-enrichment-data/enriched_signals_with_snapshot_data' USING BinStorage() AS (
            signal_key:           chararray,
            service:              chararray,
            service_account_id:   chararray,
            service_signal_id:    chararray,
            relationship_id:      chararray,
            time:                 long,
            type:                 chararray,
            text:                 chararray,
            content_type:         chararray,
            parent_signal_id:     chararray,
            conversation_id:      chararray,
            account_snapshot_id:  chararray,
            ecosystem_account_id: chararray,
            time_bucket:          long,
            network_strength:     long,
            network_size:         long
          );

filtered = FILTER signals BY signal_key IS NOT NULL;


--
-- Extract mentions
--
signal_with_mention_raw = FOREACH (FILTER filtered BY type=='post') GENERATE signal_key AS signal_key, FLATTEN(ExtractMentions(text)) AS (mentioned_screenname:chararray);
with_lowered_mention    = FOREACH signal_with_mention_raw GENERATE signal_key AS signal_key, LOWER(mentioned_screenname) AS mentioned_screenname;
mentioned_accounts      = FOREACH (JOIN with_lowered_mention BY mentioned_screenname, tweet_account_all BY name USING 'replicated') GENERATE
                            signal_key                           AS signal_key,
                            CONCAT('mention:',dachis_account_id) AS mention_column_name,
                            dachis_account_id                    AS dachis_account_id;
--
-- Classify signals and extract links
--
signals_with_links = FOREACH filtered GENERATE
                       signal_key                       AS signal_key,
                       service                          AS service,
                       service_account_id               AS service_account_id,
                       service_signal_id                AS service_signal_id,
                       relationship_id                  AS relationship_id,
                       time                             AS time,
                       type                             AS type,
                       content_type                     AS content_type,
                       parent_signal_id                 AS parent_signal_id,
                       conversation_id                  AS conversation_id,
                       account_snapshot_id              AS account_snapshot_id,
                       ecosystem_account_id             AS ecosystem_account_id,
                       time_bucket                      AS time_bucket,
                       network_strength                 AS network_strength,
                       network_size                     AS network_size,
                       ClassifySentiment(service, text) AS sentiment,
                       ExtractLinks(text)               AS links;

--
-- Finally, combine the results
--
result = FOREACH (COGROUP signals_with_links BY signal_key OUTER, mentioned_accounts BY signal_key) GENERATE
           FLATTEN(signals_with_links) AS (
             signal_key,
             service,
             service_account_id,
             service_signal_id,
             relationship_id,
             time,
             type,
             content_type,
             parent_signal_id,
             conversation_id,
             account_snapshot_id,
             ecosystem_account_id,
             time_bucket,
             network_strength,
             network_size,
             sentiment,
             links),
           mentioned_accounts.(mention_column_name, dachis_account_id) AS mentions;

STORE result INTO 'temporary-enrichment-data/classified_signals' USING BinStorage();
