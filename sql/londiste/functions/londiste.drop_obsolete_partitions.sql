
CREATE OR REPLACE FUNCTION londiste.drop_obsolete_partitions
(
  IN i_parent_table text,
  IN i_retention_period interval
)
  RETURNS SETOF text
AS $$
-------------------------------------------------------------------------------
--  Function: londiste.drop_obsolete_partitions(2)
--
--    Drop obsolete partitions of partition-by-date parent table.
--
--  Parameters:
--    i_parent_table      Master table from which partitions are inherited
--    i_retention_period  How long to keep partitions around
--
--  Returns:
--    Names of partitions dropped
-------------------------------------------------------------------------------
DECLARE
  _schema text NOT NULL := lower( split_part( i_parent_table, '.', 1));
  _table  text NOT NULL := lower( split_part( i_parent_table, '.', 2));
  _part   text;
BEGIN
  FOR _part IN
    SELECT quote_ident( t.schemaname) ||'.'|| quote_ident( t.tablename)
      FROM pg_catalog.pg_tables t
     WHERE t.schemaname = _schema
       AND t.tablename ~ ('^'|| _table ||'_[0-9]{4}_[0-9]{2}_[0-9]{2}$')
       AND t.tablename < _table || to_char( now() - i_retention_period, '_YYYY_MM_DD')
  LOOP
    EXECUTE 'DROP TABLE '|| _part;
    RETURN NEXT _part;
  END LOOP;
END;
$$ LANGUAGE plpgsql;

-- vim:et:sw=2:ts=2:nowrap:
