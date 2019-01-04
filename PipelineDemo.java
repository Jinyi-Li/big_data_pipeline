import javax.servlet.ServletContext;
import org.apache.tomcat.jdbc.pool.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;


public class PipelineDemo {

    private static DataSource dataSource;

    /**
     * Initialize the required database connection based on the current context
     * name.
     * @param servletContext current servlet context
     * @throws IOException
     */
    public void initDBConnection(ServletContext servletContext) throws IOException {

        // get context name from servlet container.
        String contextName = servletContext.getContextPath().substring(1);

        // open corresponding config file, including db config.
        File configFile;
        switch (contextName) {
            case "myapp_test":
                configFile = new File("myapp_test_config");
                break;
            case "myapp_dev":
                configFile = new File("myapp_dev_config");
                break;
            case "myapp_live":
                configFile = new File("myapp_live_config");
                break;
            default:
                System.err.println("Config file error.");
                return;
        }

        // load properties
        Properties properties = new Properties();
        properties.load(new FileInputStream(configFile));

        // get corresponding dataSource
        dataSource = (DataSource) (new DataSourceFactory().createDataSource(properties));
    }

    /**
     * Get a connection from the connection pool.
     * @return a connection instance
     */
    public Connection getConnection(){
        if(dataSource == null){
            System.err.println("DB not initiated.");
        }
        return dataSource.getConnection(autocommit=false);
    }

    /**
     * This function would be placed in the 1st level (the lowest level).
     * It handles a list of DB requests.
     */
    public List<Row> treat() {
        if (this.boundStatements.isEmpty()) {
            Log.i("WARNING: No bound statements, either this query was already treated or no statements were added"); // it means no statement needs to be treated.
        }

        try {
            Session session = CassandraConnector.getSession();

            long startTime = System.currentTimeMillis();
            List<Row> res = executeQuery(session, this.boundStatements);

            this.boundStatements.clear();
            return res;
        } catch (Exception e) {
            e.printStackTrace();
            Log.i(e.getMessage());
            this.failed = true;
            return null;
        }
    }


    /**
     * This function would be placed in the 2nd level.
     * It handles a stream of DB requests.
     */
    public void streamRows(HandlerRowList streamingHandler) {
        this.handler = streamingHandler;
        treat();
    }


    /* The below two functions would be in the 3rd level. */

    /**
     * Callback function will pass the parameter to the calling object.
     */
    public interface HandlerInteger {
        void callback(long countUserId);
    }

    /**
     * The stream method would handle the data pipeline logic.
     * @param productId
     * @param fromDate
     * @param toDateExclusive
     * @param streamCountCallback
     */
    public static void streamCountUserIds(String productId, DateTime fromDate, DateTime toDateExclusive, HandlerInteger streamCountCallback) {
        String query = "SELECT count(userID) as count FROM " + TABLE_NAME_INDEX__DAY + " WHERE pid=? AND day=? AND bucketID_day=?";
        SafeRequestCassandra request = new SafeRequestCassandra(query, 32);

        // query over full date range
        for (DateTime d = fromDate; d.isBefore(toDateExclusive); d = d.plusDays(1)) {
            DateTime dFinal = new DateTime(d);
            IntStream.range(0, UserActivityBean.BUCKET_SIZE_DAY).forEach(bucketID -> {
                request.addStatements(productId, DateUtil.toCassandraLocalDate(dFinal), bucketID);
            });
        }

        // Each unit in this stream is a
        request.streamRows(partialRows -> {
            long count = partialRows.stream().map(row -> row.getLong("count")).mapToLong(a -> a.longValue()).sum();
            streamCountCallback.callback(count);
        });
    }


    /*
     * This function is written in highest-level (DAO.java).
     * You pass a lambda function after the interface paramater.
     * Each row in the stream will first be processed in 'streamCountUserIds' function,
     * then the callback function in 'streamCountUserIds' will pass its parameter back to the lambda function.
     * So you will get each count from stream and do whatever you want in the lambda function.
     */
    public static void synchronizeDAUByCount(String productId, DateTime from, DateTime toDateExclusive) {
        for (DateTime d = from; d.isBefore(toDateExclusive); d = d.plusDays(1)) {
            DailyBean bean = new DailyBean();
            streamCountUserIds(productId, d, d.plusDays(1), streamCountCallback -> {
                bean.allDau += streamCountCallback;
            });
        }
    }

}
