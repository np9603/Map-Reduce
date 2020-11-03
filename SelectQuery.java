import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

public class SelectQuery {
	

	public static class SelectMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		
		HashMap<String, Integer> col_index = new HashMap<>();
		HashMap<String, String> valueType = new HashMap<>();

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

			String line = value.toString();
			String[] columns = line.split(",");
			
			LongWritable primarykey = new LongWritable(key.get());
			StringBuilder sb = new StringBuilder();
			
			Configuration conf = context.getConfiguration();

			ArrayList<String> displayTerms = new ArrayList<String>(
					Arrays.asList(conf.getStrings("selectionTerms")));

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
				for (int i = 0; i < displayTerms.size(); i++) {
					sb.append(columns[col_index.get(displayTerms.get(i))] + ",");
				}
				
				Text outputValue = new Text(sb.substring(0, sb.length() - 1)
						.toString().trim());
				context.write(primarykey, outputValue);
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

	public static class SelectReducer extends
			Reducer<LongWritable, Text, LongWritable, Text> {
		public void reduce(LongWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {

			StringBuilder finalValue = new StringBuilder();
			for (Text val : values) {
				finalValue.append(val.toString());
			}
			
			Text finalWord = new Text(finalValue.toString().trim());
			new IntWritable(0);
			NullWritable.get();
			context.write(key, finalWord);
			
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
			job.setJarByClass(SelectQuery.class);
			job.setMapperClass(SelectMapper.class);
			job.setCombinerClass(SelectReducer.class);
			job.setReducerClass(SelectReducer.class);

			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(LongWritable.class);
			job.setOutputValueClass(Text.class);
			
			FileSystem fs = FileSystem.get(new Configuration());
			
			fs.delete(new Path("/output"), true);
			
			FileInputFormat.addInputPath(job, new Path(args[1]));
			FileOutputFormat.setOutputPath(job, new Path(args[2]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);

			
		}
	}
}
