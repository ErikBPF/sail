@sail-only
Feature: Delta Lake OPTIMIZE

  Rule: Compaction

    Background:
      Given variable location for temporary directory delta_optimize_compaction
      Given final statement
        """
        DROP TABLE IF EXISTS delta_optimize_compaction
        """
      Given statement template
        """
        CREATE TABLE delta_optimize_compaction (id INT, value STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_optimize_compaction VALUES (1, 'a')
        """
      Given statement
        """
        INSERT INTO delta_optimize_compaction VALUES (2, 'b')
        """
      Given statement
        """
        INSERT INTO delta_optimize_compaction VALUES (3, 'c')
        """

    Scenario: OPTIMIZE compacts small files without changing rows
      Then delta active data files count is 3
      Given statement
        """
        OPTIMIZE delta_optimize_compaction
        """
      Then delta active data files count is 1
      When query
        """
        SELECT * FROM delta_optimize_compaction ORDER BY id
        """
      Then query result ordered
        | id | value |
        | 1  | a     |
        | 2  | b     |
        | 3  | c     |
      Then delta log latest commit info contains
        | path                              | value      |
        | operation                         | "OPTIMIZE" |
        | operationMetrics.numAddedFiles    | 1          |
        | operationMetrics.numRemovedFiles  | 3          |
      Then delta log latest commit rewrites files without data changes

    Scenario: Repeating compaction is idempotent
      Given statement
        """
        OPTIMIZE delta_optimize_compaction
        """
      Given statement
        """
        OPTIMIZE delta_optimize_compaction
        """
      Then delta active data files count is 1
      Then delta log commit count is 5

    Scenario: OPTIMIZE accepts a quoted Delta path
      Given statement template
        """
        OPTIMIZE {{ location.sql }}
        """
      Then delta active data files count is 1

  Rule: Partition filtering

    Background:
      Given variable location for temporary directory delta_optimize_partition
      Given final statement
        """
        DROP TABLE IF EXISTS delta_optimize_partition
        """
      Given statement template
        """
        CREATE TABLE delta_optimize_partition (id INT, bucket INT)
        USING DELTA
        PARTITIONED BY (bucket)
        LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_optimize_partition VALUES (1, 0), (2, 1)
        """
      Given statement
        """
        INSERT INTO delta_optimize_partition VALUES (3, 0), (4, 1)
        """

    Scenario: WHERE compacts only selected partitions
      Then delta active data files count is 4
      Given statement
        """
        OPTIMIZE delta_optimize_partition WHERE bucket = 0
        """
      Then delta active data files count is 3
      When query
        """
        SELECT * FROM delta_optimize_partition ORDER BY id
        """
      Then query result ordered
        | id | bucket |
        | 1  | 0      |
        | 2  | 1      |
        | 3  | 0      |
        | 4  | 1      |

    Scenario: Repeating compaction is idempotent across partitions
      Given statement
        """
        OPTIMIZE delta_optimize_partition
        """
      Given statement
        """
        OPTIMIZE delta_optimize_partition
        """
      Then delta active data files count is 2
      Then delta log commit count is 4

    Scenario: WHERE rejects predicates on non-partition columns
      Given statement with error partition
        """
        OPTIMIZE delta_optimize_partition WHERE id = 1
        """

    Scenario: ZORDER BY rejects a partition column
      Given statement with error Z-Ordering column.*cannot be a partition column
        """
        OPTIMIZE delta_optimize_partition ZORDER BY (bucket)
        """

  Rule: Z-ordering

    Background:
      Given variable location for temporary directory delta_optimize_zorder
      Given final statement
        """
        DROP TABLE IF EXISTS delta_optimize_zorder
        """
      Given statement template
        """
        CREATE TABLE delta_optimize_zorder (id INT, event_type STRING)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement
        """
        INSERT INTO delta_optimize_zorder VALUES (3, 'view')
        """
      Given statement
        """
        INSERT INTO delta_optimize_zorder VALUES (1, 'click')
        """
      Given statement
        """
        INSERT INTO delta_optimize_zorder VALUES (2, 'view')
        """

    Scenario: ZORDER BY compacts files and preserves rows
      Given statement
        """
        OPTIMIZE delta_optimize_zorder ZORDER BY (event_type, id)
        """
      Then delta active data files count is 1
      When query
        """
        SELECT * FROM delta_optimize_zorder ORDER BY id
        """
      Then query result ordered
        | id | event_type |
        | 1  | click      |
        | 2  | view       |
        | 3  | view       |
      Then delta log latest commit rewrites files without data changes

    Scenario: ZORDER BY rejects an unknown column
      Given statement with error Z-Ordering column.*does not exist
        """
        OPTIMIZE delta_optimize_zorder ZORDER BY (missing)
        """

  Rule: FULL mode

    Scenario: FULL is rejected for a table without liquid clustering
      Given variable location for temporary directory delta_optimize_full
      Given final statement
        """
        DROP TABLE IF EXISTS delta_optimize_full
        """
      Given statement template
        """
        CREATE TABLE delta_optimize_full (id INT)
        USING DELTA LOCATION {{ location.sql }}
        """
      Given statement with error OPTIMIZE FULL is only supported for clustered tables
        """
        OPTIMIZE delta_optimize_full FULL
        """
