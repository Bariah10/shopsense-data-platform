# Great Expectations suites that gate the ShopSense pipeline.
import great_expectations as gx
from great_expectations import expectations as gxe

ALLOWED_CATEGORIES = ['electronics', 'grocery', 'fashion', 'home', 'beauty', 'sports']
ALLOWED_STATUSES   = ['created', 'paid', 'shipped', 'delivered', 'cancelled']
ALLOWED_CURRENCIES = ['SAR', 'USD', 'AED']


class QualityGateFailed(Exception):
    # Raised when a suite fails. Airflow turns this into a failed task,
    # which puts every downstream task into upstream_failed.
    def __init__(self, layer, failures, summary):
        self.layer, self.failures, self.summary = layer, failures, summary
        lines = '\n'.join(f'  - {f["expectation"]} on {f["column"]}: '
                          f'{f["unexpected_count"]} unexpected value(s)' for f in failures)
        super().__init__(f'Quality gate FAILED on {layer}: '
                         f'{len(failures)}/{summary["evaluated"]} expectations failed\n{lines}')


def bronze_expectations():
    return [
        gxe.ExpectColumnValuesToNotBeNull(column='order_id'),
        gxe.ExpectColumnValuesToNotBeNull(column='customer_id'),
        gxe.ExpectColumnValuesToBeBetween(column='quantity', min_value=1, max_value=100),
        gxe.ExpectColumnValuesToBeBetween(column='unit_price', min_value=0.01),
        gxe.ExpectColumnValuesToBeInSet(column='currency', value_set=ALLOWED_CURRENCIES),
        gxe.ExpectColumnValuesToBeInSet(column='status',   value_set=ALLOWED_STATUSES),
        gxe.ExpectColumnValuesToBeInSet(column='category', value_set=ALLOWED_CATEGORIES),
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
    ]


def silver_expectations():
    return [
        gxe.ExpectColumnValuesToBeUnique(column='order_id'),      # the MERGE must hold the key unique
        gxe.ExpectColumnValuesToNotBeNull(column='order_id'),
        gxe.ExpectColumnValuesToNotBeNull(column='customer_id'),
        gxe.ExpectColumnValuesToBeBetween(column='line_total', min_value=0.01),
        gxe.ExpectColumnValuesToBeInSet(column='status', value_set=ALLOWED_STATUSES),
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
    ]


SUITES = {'bronze': bronze_expectations, 'silver': silver_expectations}


def validate(df, layer):
    # Runs the suite for `layer` against a pandas DataFrame and returns (success, failures, summary).
    expectations = SUITES[layer]()
    ctx    = gx.get_context(mode='ephemeral')
    source = ctx.data_sources.add_pandas(f'{layer}_source')
    asset  = source.add_dataframe_asset(name=f'{layer}_orders')
    batch  = asset.add_batch_definition_whole_dataframe('whole_dataframe')

    suite = ctx.suites.add(gx.ExpectationSuite(name=f'{layer}_suite'))
    for exp in expectations:
        suite.add_expectation(exp)

    vdef = ctx.validation_definitions.add(
        gx.ValidationDefinition(name=f'{layer}_validation', data=batch, suite=suite))
    result = vdef.run(batch_parameters={'dataframe': df})

    failures, checks = [], []
    for r in result.results:
        cfg = r.expectation_config
        row = {'expectation': cfg.type,
               'column': cfg.kwargs.get('column', '(table)'),
               'success': bool(r.success),
               'unexpected_count': int(r.result.get('unexpected_count', 0) or 0)}
        checks.append(row)
        if not r.success:
            failures.append(row)

    summary = {'layer': layer, 'rows': int(len(df)), 'evaluated': len(checks),
               'passed': len(checks) - len(failures), 'failed': len(failures),
               'success': bool(result.success), 'checks': checks}
    return bool(result.success), failures, summary


def gate(df, layer):
    # Validate and STOP the pipeline if the data is not fit for the next stage.
    ok, failures, summary = validate(df, layer)
    if not ok:
        raise QualityGateFailed(layer, failures, summary)
    return summary
