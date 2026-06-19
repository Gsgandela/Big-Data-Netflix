import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class NetflixAnalysis {

    // ------------------------------------
    // MAPPER
    // ------------------------------------
    public static class NetflixMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Lista simples de stopwords em inglês (já que o dataset é inglês). Desafio Extra!
        private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList(
                "a", "an", "the", "and", "or", "but", "is", "are", "in", "on", "to", "with",
                "of", "for", "it", "as", "that", "this", "by", "from", "his", "her", "at", 
                "when", "who", "an", "be", "their", "into"
        ));

        private final static IntWritable one = new IntWritable(1);
        private Text wordText = new Text();
        private Text titleText = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Pula o cabeçalho do CSV
            if (key.get() == 0 && value.toString().contains("show_id")) return;

            String line = value.toString();
            // Regex para separar as colunas do CSV respeitando campos entre aspas
            String[] columns = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (columns.length >= 12) {
                // A coluna de título geralmente é a índice 2 e a descrição a 11
                String title = columns[2].replaceAll("\"", "").trim();
                String description = columns[11].replaceAll("\"", "").trim();

                // 1. Normalização do Texto
                description = description.toLowerCase(); // Tudo minúsculo
                description = description.replaceAll("[^a-z0-9\\s]", " "); // Remove pontuação/especiais

                String[] words = description.split("\\s+");
                int validWordCount = 0;

                for (String word : words) {
                    if (!word.isEmpty()) {
                        validWordCount++; // Conta todas as palavras para o título
                        
                        // Ignora stopwords na contagem de frequência (Desafio Extra)
                        if (!STOPWORDS.contains(word)) {
                            wordText.set("W:" + word); // Prefixo 'W:' para indicar que é uma palavra
                            context.write(wordText, one);
                        }
                    }
                }

                // Envia o tamanho da descrição do título
                titleText.set("T:" + title); // Prefixo 'T:' para indicar título
                context.write(titleText, new IntWritable(validWordCount));
            }
        }
    }

    // ------------------------------------
    // REDUCER
    // ------------------------------------
    public static class NetflixReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Map<String, Integer> wordCounts = new HashMap<>();
        private int totalWords = 0;

        private String longestTitle = "";
        private int maxWords = -1;

        private String shortestTitle = "";
        private int minWords = Integer.MAX_VALUE;

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            String keyStr = key.toString();

            if (keyStr.startsWith("W:")) {
                // É uma palavra (Word)
                String word = keyStr.substring(2);
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                wordCounts.put(word, wordCounts.getOrDefault(word, 0) + sum);
                totalWords += sum; // Vai acumulando o total de palavras úteis

            } else if (keyStr.startsWith("T:")) {
                // É um título
                String title = keyStr.substring(2);
                int descLength = values.iterator().next().get(); 

                if (descLength > maxWords) {
                    maxWords = descLength;
                    longestTitle = title;
                }
                // Garante que não conte filmes com descrição vazia ou erros do CSV
                if (descLength < minWords && descLength > 0) {
                    minWords = descLength;
                    shortestTitle = title;
                }
            }
        }

        // Método chamado no fim do processamento do Reducer
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Ordena o mapa de palavras pela frequência
            List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCounts.entrySet());
            list.sort((a, b) -> b.getValue().compareTo(a.getValue())); // Decrescente

            // 1. Título com descrição mais longa
            context.write(new Text("TÍTULO COM DESCRIÇÃO MAIS LONGA:"), new Text(longestTitle + " (" + maxWords + " palavras)"));
            
            // 2. Título com descrição mais curta
            context.write(new Text("TÍTULO COM DESCRIÇÃO MAIS CURTA:"), new Text(shortestTitle + " (" + minWords + " palavras)"));
            
            // 3. Número total de palavras
            context.write(new Text("TOTAL DE PALAVRAS PROCESSADAS:"), new Text(String.valueOf(totalWords)));

            // 4. Top 5 palavras MAIS repetidas
            context.write(new Text("--- TOP 5 MAIS REPETIDAS ---"), new Text(""));
            for (int i = 0; i < Math.min(5, list.size()); i++) {
                context.write(new Text((i+1) + ". " + list.get(i).getKey()), new Text(String.valueOf(list.get(i).getValue())));
            }

            // 5. Top 5 palavras MENOS repetidas
            context.write(new Text("--- TOP 5 MENOS REPETIDAS ---"), new Text(""));
            int size = list.size();
            for (int i = 0; i < Math.min(5, size); i++) {
                Map.Entry<String, Integer> entry = list.get(size - 1 - i);
                context.write(new Text((i+1) + ". " + entry.getKey()), new Text(String.valueOf(entry.getValue())));
            }
        }
    }

    // ------------------------------------
    // MAIN
    // ------------------------------------
// ------------------------------------
    // MAIN
    // ------------------------------------
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // ESTA LINHA FORÇA O MODO LOCAL DENTRO DO CÓDIGO (Sem precisar digitar no CMD!)
        conf.set("fs.defaultFS", "file:///");
        conf.set("dfs.permissions.enabled", "false");
        Job job = Job.getInstance(conf, "Netflix Analysis");
        
        job.setJarByClass(NetflixAnalysis.class);
        job.setMapperClass(NetflixMapper.class);
        job.setReducerClass(NetflixReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}