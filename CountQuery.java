import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountQuery {

	public static class CountMapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		HashMap<String, String> valueType = new HashMap<>();
		HashMap<String, Integer> col_index = new HashMap<>();

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			col_index.put("id", 0);
			col_index.put("name", 1);
			col_index.put("countrycode", 2);
			col_index.put("district", 3);
			col_index.put("population", 4);

			valueType.put("id", "INTEGER");
			valueType.put("name", "STRING");
			valueType.put("countrycode", "STRING");
			valueType.put("district", "STRING");
			valueType.put("population", "INTEGER");
			valueType.put("dateofbirthyear", "INTEGER");
			if (key.get() != 0) {
				String line = value.toString();
				String[] columns = line.split(",");
				String word = "COUNT";
				Configuration conf = context.getConfiguration();
				ArrayList<String> whereTerms = new ArrayList<String>(
						Arrays.asList(conf.getStrings("whereTerms")));

				boolean whereResult = true;
				boolean[] andOr = new boolean[2];
				andOr[0] = conf.getBoolean("andConditions", false);
				andOr[1] = conf.getBoolean("orConditions", false);
				if (conf.getBoolean("isWhere", false) && key.get() != 0) {
					whereResult = implementWhere(columns, whereTerms, andOr);
					
				}
				if (whereResult) {
					if (conf.getBoolean("isGroupBy", false)) {
						word = columns[col_index.get(conf.get("groupByField"))];
					}
					Text outputKey = new Text(word);
					IntWritable outputValue = new IntWritable(1);
					context.write(outputKey, outputValue);
				}
			}
		}

		public boolean implementWhere(String[] columns,
				ArrayList<String> conditions, boolean[] andOr) {
			if (conditions.size() == 1)
				return true;
			boolean finalResult = true;
			boolean currentResult = false;
			for (int i = 0; i < conditions.size() - 2; i += 3) {
				String leftOperand = conditions.get(i);
				String Operator = conditions.get(i + 1);
				String rightOperand = conditions.get(i + 2);
				currentResult = false;
				if ((valueType.get(leftOperand)).equals("INTEGER")) {
					int leftOperandValue = Integer.parseInt(columns[col_index
							.get(leftOperand)]);
					int rightOperandValue = Integer.valueOf(rightOperand);
					switch (Operator) {
					case "=":
						currentResult = leftOperandValue == rightOperandValue;
						break;
					case "<>":
						currentResult = leftOperandValue != rightOperandValue;
						break;
					case ">":
						currentResult = leftOperandValue > rightOperandValue;
						break;
					case "<":
						currentResult = leftOperandValue < rightOperandValue;
						break;
					case ">=":
						currentResult = leftOperandValue >= rightOperandValue;
						break;
					case "<=":
						currentResult = leftOperandValue <= rightOperandValue;
						break;
					default:
						currentResult = false;
					}
				} else if ((valueType.get(leftOperand)).equals("STRING")) {
					String leftOperandValue = columns[col_index
							.get(leftOperand)].trim();
					String rightOperandValue = rightOperand.substring(1,
							rightOperand.length() - 1).trim();
					switch (Operator) {
					case "=":
						currentResult = leftOperandValue
								.equals(rightOperandValue);
						break;
					case "<>":
						currentResult = !leftOperandValue
								.equals(rightOperandValue);
						break;
					default:
						currentResult = false;
					}
				}
				if (andOr[0])
					finalResult = finalResult && currentResult;
				else if (andOr[1])
					finalResult = finalResult || currentResult;
				else
					finalResult = currentResult;
			}
			return finalResult;

		}

	}

	public static class CountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {

		
		String SQL = args[0];
		SQL = SQL.toLowerCase();

		Configuration conf = new Configuration();

		String[] selecitonTerms = new String[1];
		String[] whereConditions = new String[1];

		ArrayList<String> list = new ArrayList<String>();

		if (SQL.charAt(7) == '*') {
			list.add("id");
			list.add("name");
		} else {
			selecitonTerms = SQL
					.substring(SQL.indexOf("select ") + 7, SQL.indexOf("from"))
					.replaceAll("^[,\\s]+", "").split("[,\\s]+");

			for (String val : selecitonTerms) {
				list.add(val);

			}

			String[] arr = list.toArray(new String[list.size()]);
			conf.setStrings("selectionTerms", arr);

			

			if (SQL.contains("where") && SQL.contains("group by")) {
				whereConditions = SQL
						.substring(SQL.indexOf("from ") + 5,
								SQL.indexOf("group by"))
						.replaceAll("^[,\\s]+", "").split("[,\\s]+");
				ArrayList<String> condition_keys = new ArrayList<>(
						Arrays.asList(whereConditions));

				if (condition_keys.contains("and"))
					conf.setBoolean("andConditions", true);
				if (condition_keys.contains("or"))
					conf.setBoolean("orConditions", true);

				while (condition_keys.remove("and")
						|| condition_keys.remove("or")) {
				}

				conf.setBoolean("isWhere", true);

				
				String[] conditions = condition_keys
						.toArray(new String[condition_keys.size()]);
				conf.setStrings("whereTerms", conditions);
			} else if (SQL.contains("where") && !SQL.contains("group by")) {
				whereConditions = SQL.substring(SQL.indexOf("where") + 6)
						.replaceAll("^[,\\s]+", "").split("[,\\s]+");
				ArrayList<String> condition_keys = new ArrayList<>(
						Arrays.asList(whereConditions));
				if (condition_keys.contains("and"))
					conf.setBoolean("andConditions", true);
				if (condition_keys.contains("or"))
					conf.setBoolean("orConditions", true);
				while (condition_keys.remove("and")
						|| condition_keys.remove("or")) {
				}
				conf.setBoolean("isWhere", true);
				String[] conditions = condition_keys
						.toArray(new String[condition_keys.size()]);
				
				System.out.println("Here: " + conditions.length);
				conf.setStrings("whereTerms", conditions);
			} else {
				String[] conditions = new String[1];
				conditions[0] = "temp";
				conf.setStrings("whereTerms", conditions);
			}

			if (SQL.contains("group by")) {
				String[] groupByField = SQL.split("group by ");
				conf.setBoolean("isGroupBy", true);
				conf.set("groupByField", groupByField[1].trim());
			}
			
			Job job = Job.getInstance(conf, "word count");
			job.setJarByClass(CountQuery.class);
			job.setMapperClass(CountMapper.class);
			job.setCombinerClass(CountReducer.class);
			job.setReducerClass(CountReducer.class);

			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			FileSystem fs = FileSystem.get(new Configuration());
			
			fs.delete(new Path("/output"), true);

			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);
			Runtime rt = Runtime.getRuntime();
			Process proc = rt.exec("rm output.txt");
			proc = rt.exec("hdfs dfs -get /output/part-r-00000 output.txt");

		}
	}
}