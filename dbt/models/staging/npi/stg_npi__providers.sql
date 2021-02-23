SELECT  
	  npi               AS npi
	, entity_type_code  AS entity_type_code
	, organization_name AS organization_name
	, last_name         AS last_name
	, first_name        AS first_name
	, state             AS state_abbreviation
	, taxonomy_code     AS taxonomy_code
FROM {{ source('great_expectations', 'npi_small') }}