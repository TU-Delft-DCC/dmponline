import pandas as pd
import argparse
import os
import sys
import logging
import json

from dmponline import DMPonline


def question_overview(plan_id, api, output_file=None):
    df_list = []
    for item in api.get_plan_v0(plan_id).plan_content[0][0]['sections']:
        section_number = item['number']
        section_df = pd.json_normalize(item['questions'])
        section_df['section'] = section_number
        df_list.append(section_df)

    df_overview = pd.concat(df_list).loc[:, ['section', 'number', 'text', 'format', 'option_based']].rename(columns={'number': 'question'})

    if output_file is not None:
        extension = os.path.splitext(output_file)[-1]
        if extension == '.html':
            # save to html file
            df_overview.to_html(open(output_file, 'w'), index=False)
        elif extension == '.csv':
            # save to csv file
            df_overview.to_csv(open(output_file, 'w'), index=False)
        elif extension == '.xlsx':
            # save to excel file
            df_overview.to_excel(output_file, index=False)
        else:
            # other extensions are not supported: throw warning
            logging.warning(f'Extension {extension} not supported')
    else:
        # print as html
        print(df_overview.to_html(index=False))


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = argparse.ArgumentParser(
        description='DMPonline question overview for particular plan, specified by the plan ID')
    # required named arguments
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('-i', '--dmponline-plan-id', type=int, help='DMPonline plan ID.', required=True)
    requiredNamed.add_argument('-t', '--dmponline-api-token', help='DMPonline API access token.', required=True)

    # optional arguments
    parser.add_argument('-u', '--dmponline-user-email', default=None, help='Username (email) of user corresponding to DMPONLINE_API_TOKEN (required for API v1 requests).')
    parser.add_argument('--do-not-verify', action='store_true', help='boolean, overridden by --cert-file argument if provided.')
    parser.add_argument('-c', '--cert-file', default=None, help='SSL certificate file.')
    parser.add_argument('-o', '--output-file', default=None, help='Output file to write to (supported formats: .html, .csv, .xlsx).')

    args = parser.parse_args()

    if args.cert_file is not None and os.path.exists(args.cert_file):
        verify = args.cert_file
    else:
        verify = not args.do_not_verify
    logging.debug('call api with verify={}'.format(verify))
    dmp_api = DMPonline(args.dmponline_api_token, verify=verify, token_user=args.dmponline_user_email)

    question_overview(args.dmponline_plan_id, dmp_api,
                      output_file=args.output_file)

if __name__ == '__main__':
    main()
