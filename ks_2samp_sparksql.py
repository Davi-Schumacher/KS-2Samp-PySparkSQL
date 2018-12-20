import pyspark.sql.functions as funcs
from pyspark.sql.window import Window

CDF_1 = 'cdf_1'
CDF_2 = 'cdf_2'
FILLED_CDF_1 = 'filled_cdf_1'
FILLED_CDF_2 = 'filled_cdf_2'


def get_cdf(df, variable, col_name):

	cdf = df.select(variable).na.drop().\
		withColumn(
			col_name,
			funcs.cume_dist().over(Window.orderBy(variable))
		).distinct()

	return cdf


def ks_2samp(df1, df2, variable):

	ks_stat = get_cdf(df1, variable, CDF_1).\
		join(
			get_cdf(df2, variable, CDF_2),
			on=variable,
			how='outer'
		).\
		withColumn(
			FILLED_CDF_1,
			funcs.last(funcs.col(CDF_1), ignorenulls=True).
			over(Window.rowsBetween(Window.unboundedPreceding, Window.currentRow))
		).\
		withColumn(
			FILLED_CDF_2,
			funcs.last(funcs.col(CDF_2), ignorenulls=True).
			over(Window.rowsBetween(Window.unboundedPreceding, Window.currentRow))
		).\
		select(
			funcs.max(
				funcs.abs(
					funcs.col(FILLED_CDF_1) - funcs.col(FILLED_CDF_2)
				)
			)
		).\
		collect()[0][0]

	return ks_stat
