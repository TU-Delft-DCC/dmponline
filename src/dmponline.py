import logging
import re
import requests
import json
import pandas as pd
from datetime import date, timedelta
from pandas.tseries.offsets import BDay
from pandas import json_normalize


class DMPonline:
    url = 'https://dmponline.dcc.ac.uk/api/'
    token = None

    def __init__(self, token, token_user=None, verify=True):
        self.token = token
        self.verify = verify
        if token_user:
            self.bearer_token = self.get_bearer_token(token_user)
        else:
            logging.warning('api v1 not available, because token_user is not provided')

    def get_bearer_token(self, token_user):
        headers = {'Accept': 'application/json',
                   'Content-Type': 'application/x-www-form-urlencoded;charset=UTF-8'}
        params = {'grant_type': 'authorization_code',
                  'email': token_user,
                  'code': self.token}
        r = requests.post('{}v1/authenticate'.format(self.url),
                          json=params, verify=self.verify,
                          headers=headers)
        return json.loads(r.text)['access_token']

    def _last_businessday(self):
        """
        Derive last business day w.r.t. today
        :return: date string %Y-%m-%d
        """
        last_bday = date.today() - BDay(1)
        return last_bday.strftime('%Y-%m-%d')

    def _yesterday(self):
        """
        Derive yesterday
        :return: date string %Y-%m-%d
        """
        yesterday = date.today() - timedelta(days=1)
        return yesterday.strftime('%Y-%m-%d')

    def get(self, request='v0/statistics/plans', params=dict(remove_tests='true')):
        """
        get call to DMPonline API
        :param request: request string (excluding https://.../api/)
        :param params: dictionary with params to parse
        :return: json object (if http status code == 200, None otherwise)
        """
        url = self.url + request
        headers = {'Authorization': 'Token token={}'.format(self.token),
                   'Content-Type': 'application/json'}
        if 'api/v1' in url:
            headers['Authorization'] = 'Bearer {}'.format(self.bearer_token)
        try:
            r = requests.get(url,
                             headers=headers,
                             params=params,
                             verify=self.verify
                             )
        except:
            logging.error('{} does not give valid response'.format(url))
            return None

        if r.status_code == 200:
            json_text = r.text
            data = json.loads(json_text)
            return data
        else:
            logging.error('{} gives status code {}\n{}'.format(url, r.status_code, r.text))
            return None

    @staticmethod
    def process_users(x, key='email'):
        return '; '.join([item[key] for item in json.loads(str(x).replace("'", '"')) if key in item])

    def get_plan(self, plan_id):
        """
        Retreive plan from API
        :param plan_id:
        :return:
        """
        try:
            # the api/v0/plans endpoint fails for quite a number of plans for unclear reasons
            df = self.get_plan_v0(plan_id)
        except requests.exceptions.ConnectionError:
            # fall back to api/v1/plans endpoint
            df = self.get_plan_v1(plan_id)

        return df

    def get_plan_v0(self, plan_id):
        """
        Retreive plan from API v0
        :param plan_id: plan id integer
        :return: dataframe with plan details
        """
        # the api/v0/plans endpoint fails for quite a number of plans for unclear reasons
        request = 'v0/plans?plan={}'.format(plan_id)
        data = self.get(request=request, params={'remove_tests': 'false'})
        if data == [] or data is None:
            return None
        df = json_normalize(data)
        df.creation_date = pd.to_datetime(df.creation_date)
        df.last_updated = pd.to_datetime(df.last_updated)
        df.rename(columns={'principal_investigator.email': 'email_pi'}, inplace=True)
        df.users = df.users.apply(lambda x: self.process_users(x, key='email'))

        return df

    def get_plan_v1(self, plan_id):
        """
        Retreive plan from API v1
        :param plan_id: plan id integer
        :return: dataframe with plan details
        """
        # fall back to api/v1/plans endpoint
        request = 'v1/plans/{}'.format(plan_id)
        data = self.get(request=request, params={'remove_tests': 'false'})
        df = json_normalize(data['items'][0]['dmp'])
        df.created = pd.to_datetime(df.created)
        df.modified = pd.to_datetime(df.modified)
        if 'contributor' in df:
            try:
                logging.debug('read contributors')
                df.contributor = df.contributor.apply(lambda x: self.process_users(x, key='mbox'))
            except:
                logging.debug(f'likely a special symbol occurs in a contributor\'s name of plan {plan_id}, e.g. \'.')

                # fall back in case of e.g. a ' in the name
                def process_contributor(x):
                    return '; '.join(re.findall('[a-z.-]+@[a-z.-]+', str(x), re.I))

                df.contributor = df.contributor.apply(lambda x: process_contributor(x))

        return df

    def dmp_count(self):
        """
        Derive number of DMPs
        :return: integer indicating the number of DMPs
        """
        data = self.get()
        df = json_normalize(data['plans'])
        logging.debug('Number of plans: {}'.format(df.shape[0]))
        return df.shape[0]

    def plan_statistics(self, params=None):
        """
        Generic metadata about all plans created by all users from your organisation as pandas dataframe
        :param params: API arguments
        :return: plan statistics as pandas dataframe
        """
        if params is None:
            params = {'remove_tests': 'true'}
        request = 'v0/statistics/plans'
        data = self.get(request=request, params=params)
        if data is None:
            return None
        df = json_normalize(data['plans'])
        # df.rename(columns={'owner.email': 'email'}, inplace=True)
        df.date_created = pd.to_datetime(df.date_created)
        df.date_last_updated = pd.to_datetime(df.date_last_updated)
        return df

    def get_departments(self):
        """
        Get list of your organization's departments
        :return: The list of your organization's departments as pandas dataframe
        """
        request = 'v0/departments'
        data = self.get(request=request, params={})
        df = pd.json_normalize(data)

        return df

    def get_department_users(self):
        """
        Get list of users by department
        :return: The list of departments and the users (email address) currently associated with those departments as pandas dataframe
        """
        request = 'v0/departments/users'
        data = self.get(request=request, params={})
        df_list = []
        for _, row in pd.json_normalize(data).iterrows():
            df_fac = pd.DataFrame(row.users)
            df_fac['faculty_en'] = row.code
            df_list.append(df_fac)
        df = pd.concat(df_list)
        return df

    def has_personal_data(self, plan_id, verbose=False):
        """
        Determine whether personal data is associated to the DMP
        :param plan_id: identifier of DMP
        :param verbose: Boolean related to debug logging
        :return: Boolean indicating whether personal data is associated to the DMP
        """
        personal_data = None
        parsed = self.get('v0/plans?plan={}'.format(plan_id), params={'remove_tests': 'false'})
        template_id = parsed[0]['template']['id']
        plan_content = parsed[0]['plan_content'][0]
        if template_id == 975303870:
            # TU Delft Data Management Plan template (2021)
            section_number = 5
            question_number = 2  # personal data
        elif template_id == 1753695087:
            # Data Management Plan NWO (September 2020)
            section_number = 5
            question_number = 1  # personal data
        elif template_id == 1506827492:
            # NWO Data Management Plan (January 2020)
            section_number = 5
            question_number = 1  # personal data
        elif template_id == 1461074155:
            # Data management ZonMw-template 2019
            section_number = 2
            question_number = 1
        elif template_id == 1165855271:
            # TU Delft Data Management Questions
            section_number = 1
            question_number = 10  # human subjects
        else:
            logging.warning(f'plan_id {plan_id}, having template_id {template_id}, not supported by function has_personal_data')
            return None

        for section in plan_content['sections']:
            if section['number'] == section_number:
                for question in section['questions']:
                    if question['number'] == question_number:
                        break
                break

        if question['answered']:
            if verbose:
                logging.info(f"Template: {parsed[0]['template']['title']}")
                logging.info(question['text'])
                logging.info(question['answer']['options'][0]['text'])
            personal_data_str = \
            question['answer']['options'][0]['text']
            if personal_data_str == 'Yes':
                personal_data = True
            elif personal_data_str == 'No':
                personal_data = False

        return personal_data
