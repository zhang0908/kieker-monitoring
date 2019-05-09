package kieker.monitoring.probe.spring.executions;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.TablesNamesFinder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author IcedSoul
 */
public class CommonUtils {

    public static List<String> parseTableNamesFromSql(String sql, String dataBaseName) {
        List<String> tableList = new ArrayList<String>();
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
            List<String> tables = tablesNamesFinder.getTableList(statement);
            for(String table : tables){
                tableList.add(dataBaseName + "." + table);
            }
        } catch (JSQLParserException e) {
            try {
                return parseTableNamesFromSql2(sql, dataBaseName);
            } catch (Exception e2){
                e2.printStackTrace();
            }

        }

        return tableList;
    }

    private static List<String> parseTableNamesFromSql2(String sql, String dataBaseName) throws SQLException {

        List<String> tableList = new ArrayList<String>();

        sql = sql.toLowerCase().replaceAll("\n", " ");

        String[] regexs = {"from\\s+(.*)\\s+where?", "update\\s+(.*)\\s+set", "delete from\\s+(.*)\\s+where"};//, "insert into\\s+(.*)\\("};

        for (String regex : regexs) {

            Pattern pattern = Pattern.compile(regex);

            Matcher matcher = pattern.matcher(sql);

            while (matcher.find()) {

                String tname = matcher.group(1);

                String[] sps1 = tname.trim().split(",");

                for (String sp1 : sps1) {

                    String[] sps2 = sp1.trim().split("join");

                    for (String sp2 : sps2) {

                        tableList.add(dataBaseName + "." + sp2.trim().split(" ")[0].toUpperCase());

                    }

                }

            }

        }

        if (sql.startsWith("insert into ")) {

            tableList.add(sql.substring("insert into ".length(), sql.indexOf("(")));
        }

        return tableList;

    }


}