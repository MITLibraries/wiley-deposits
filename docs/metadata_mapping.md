## Crossref to Dublin Core metadata mapping
Metadata is retrieved from the Crossref API and is mapped to Dublin Core according to this crosswalk:

Crossref field|DC field|Field notes
------ | ------ | -------
author|dc.contributor.author|Multiple values possible, create separate field instances for each value. Names concatenated as Family, Given.
container-title|dc.relation.journal|
ISSN|dc.identifier.issn|Multiple values possible, create separate field instances for each value.
issue|mit.journal.issue|
issued|dc.date.issued|
language|dc.langauge|
original-title|dc.title.alternative|
publisher|dc.publisher|
short-title|dc.title.alternative|
subtitle|dc.title.alternative|
title|dc.title|
URL|dc.relation.isversionof|
volume|mit.journal.volume|