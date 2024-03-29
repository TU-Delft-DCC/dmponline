# DMPonline

A Python interface to the DMPonline API.
For API documentation, see [https://github.com/DMPRoadmap/roadmap/wiki/API-documentation](https://github.com/DMPRoadmap/roadmap/wiki/API-documentation).

Installation:
```shell
pip install dmponline
```

Usage example:

```python
from dmponline import DMPonline

# initialize DMPonline class
dmp_api = DMPonline(<YOUR-DMPONLINE-API-TOKEN>, token_user=<YOUR-DMPONLINE-USER-EMAIL>)
# retreive pandas dataframe with all DMPs in your organization
dmps = dmp_api.plan_statistics(params={'remove_tests': 'false'})
```

## Command line
The package also ships a command line script to retreive an overview of all questions in a particular DMP.

Usage example:

```shell
question_overview -i DMPONLINE_PLAN_ID -t <YOUR-DMPONLINE-API-TOKEN>
```