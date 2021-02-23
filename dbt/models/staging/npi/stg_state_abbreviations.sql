select 
	name as state_name,
	abbreviation as state_abbreviation
  from {{ source('great_expectations', 'state_abbreviations') }}
