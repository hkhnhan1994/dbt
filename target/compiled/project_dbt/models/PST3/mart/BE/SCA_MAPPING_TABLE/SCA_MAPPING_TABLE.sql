SELECT
  "full-auth" AS SCA_RESULT,
  "SCA used" AS SCA_REASON,
UNION ALL
SELECT
  "attempt" AS SCA_RESULT,
  "non-SCA used: reason is others" AS SCA_REASON,
UNION ALL
SELECT
  "non-authenticated" AS SCA_RESULT,
  "non-SCA used: reason is others" AS SCA_REASON,
UNION ALL
SELECT
  NULL AS SCA_RESULT,
  "non-SCA used: reason is others" AS SCA_REASON,