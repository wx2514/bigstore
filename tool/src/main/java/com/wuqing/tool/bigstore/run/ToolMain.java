package com.wuqing.tool.bigstore.run;

import com.wuqing.client.bigstore.BigstoreClient;
import com.wuqing.client.bigstore.bean.Condition;
import com.wuqing.client.bigstore.bean.DataResult;
import com.wuqing.client.bigstore.bean.ResponseData;
import com.wuqing.client.bigstore.config.Constants;
import com.wuqing.tool.bigstore.command.CommondEnum;
import com.wuqing.tool.bigstore.util.ConsoleTable;
import jline.console.ConsoleReader;
import jline.console.KeyMap;
import jline.console.completer.StringsCompleter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.Writer;
import java.util.List;

/**
 * Created by wuqing on 18/5/30.
 */
public class ToolMain {

    private final static Logger logger = LoggerFactory.getLogger(ToolMain.class);

    private static final String PRE = "bigstore> ";

    private static BigstoreClient client = null;

    private static String database = null;

    public static void main(String[] args) throws Exception {
        logger.info("start tool");
        final ConsoleReader console = new ConsoleReader();
        console.addCompleter(new StringsCompleter(CommondEnum.getNameAll()));
        final Writer out  = console.getOutput();
        console.getKeys().bind(String.valueOf(KeyMap.CTRL_S), new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                try {
                    out.append("control S\n").flush();
                    out.flush();
                    console.readLine(PRE);
                } catch (Exception e1) {
                }
            }

        });

        String line = null;
        do {
            try {
                line = console.readLine(PRE).trim();
                if (line.endsWith(";")) {
                    line = line.substring(0, line.length() - 1);
                }
                if (CommondEnum.HELP.getName().equals(line)) {
                    for (CommondEnum ce : CommondEnum.values()) {
                        out.append(ce.getName()).append("\n");
                        out.append(ce.getDesc()).append("\n");
                        out.append("\n");
                    }
                    out.flush();
                } if (CommondEnum.EXIT.getName().equals(line)) {
                    System.exit(0);
                } else if (CommondEnum.SHOW_DATABASES.getName().equals(line)) {
                    if (client == null) {
                        out.append("请先使用" + CommondEnum.CONNECT.getName() +  "命令连接数据库").append("\n").flush();
                        continue;
                    }
                    ResponseData responseData = client.showDatabases();
                    DataResult data = responseData.getData();
                    if (!responseData.isSuccess()) {
                        out.append(responseData.getErrorMsg()).append("\n").flush();
                    } else {
                        while (data.next()) {
                            for (String s : data.getRow()) {
                                out.append(s).append("\n");
                            }
                        }
                        out.flush();
                    }
                } else if (CommondEnum.SHOW_TABLES.getName().equals(line)) {
                    if (client == null) {
                        out.append("请先使用" + CommondEnum.CONNECT.getName() +  "命令连接数据库").append("\n").flush();
                        continue;
                    }
                    if (database == null) {
                        out.append("请先使用" + CommondEnum.USE.getName() +  "命令选择database").append("\n").flush();
                        continue;
                    }
                    ResponseData responseData = client.showTables();
                    DataResult data = responseData.getData();
                    if (!responseData.isSuccess()) {
                        out.append(responseData.getErrorMsg()).append("\n").flush();
                    } else {
                        while (data.next()) {
                            for (String s : data.getRow()) {
                                out.append(s).append("\n");
                            }
                        }
                        out.flush();
                    }
                } else if (line.startsWith(CommondEnum.CONNECT.getName())) {
                    String ipPort = line.substring(CommondEnum.CONNECT.getName().length()).trim();
                    String[] array = ipPort.split(":");
                    String ip = null;
                    Integer port = null;
                    if (array.length == 1) {
                        ip = array[0].trim();
                        port = Constants.PORT;
                    } else if (array.length == 2) {
                        ip = array[0].trim();
                        port = Integer.parseInt(array[1].trim());
                    }
                    if (ip == null || ip.length() == 0) {
                        out.append("command is invalid").append("\n");
                        out.append(CommondEnum.CONNECT.getDesc()).append("\n").flush();
                        continue;
                    }
                    if (client != null) {
                        client.stopClient();
                    }
                    client = new BigstoreClient(ip, port);
                } else if (line.startsWith(CommondEnum.USE.getName())) {
                    String database = line.substring(CommondEnum.USE.getName().length()).trim();
                    if (database.length() == 0) {
                        out.append("command is invalid").append("\n");
                        out.append(CommondEnum.USE.getDesc()).append("\n").flush();
                        continue;
                    }
                    ResponseData responseData = client.showDatabases();
                    DataResult data = responseData.getData();
                    List<String> databases = null;
                    if (!responseData.isSuccess()) {
                        out.append(responseData.getErrorMsg()).append("\n").flush();
                    } else {
                        if (data.next()) {
                            databases = data.getRow();
                        }
                    }
                    if (databases != null && databases.contains(database)) {
                        client.setDataBase(database);
                        ToolMain.database = database;
                    } else {
                        out.append("没有此数据库["+ database + "]\n").flush();
                    }
                } else if (line.startsWith(CommondEnum.DESC.getName())) {
                    if (client == null) {
                        out.append("请先使用" + CommondEnum.CONNECT.getName() +  "命令连接数据库").append("\n").flush();
                        continue;
                    }
                    if (database == null) {
                        out.append("请先使用" + CommondEnum.USE.getName() +  "命令选择database").append("\n").flush();
                        continue;
                    }
                    String table = line.substring(CommondEnum.DESC.getName().length()).trim();
                    if ("".equals(table)) {
                        out.append(CommondEnum.DESC.getDesc()).append("\n");
                        continue;
                    }
                    ResponseData responseData = client.descTable(table);
                    DataResult data = responseData.getData();
                    if (!responseData.isSuccess()) {
                        out.append(responseData.getErrorMsg()).append("\n").flush();
                    } else {
                        while (data.next()) {
                            out.append(data.getString(0)).append("\t").append(data.getString(1)).append("\n");
                        }
                        out.flush();
                    }
                } else if (line.toLowerCase().startsWith(CommondEnum.SELECT.getName())) {
                    Condition condition = new Condition();
                    condition.setSql(line);
                    if (client == null) {
                        out.append("请先使用" + CommondEnum.CONNECT.getName() +  "命令连接数据库").append("\n").flush();
                        continue;
                    }
                    if (database == null) {
                        out.append("请先使用" + CommondEnum.USE.getName() +  "命令选择database").append("\n").flush();
                        continue;
                    }
                    //如果没有设置limit, 则修正默认条数100
                    if (condition.getLimit() == Condition.DEFAULT_LIMIT && line.toUpperCase().indexOf(" LIMIT ") == -1) {
                        condition.setLimit(100);
                    }
                    //ConsoleTable table = new ConsoleTable(4);
                    ResponseData responseData = client.query(condition);
                    DataResult data = responseData.getData();
                    if (!responseData.isSuccess()) {
                        out.append(responseData.getErrorMsg()).append("\n").flush();
                    } else {
                        ConsoleTable table = new ConsoleTable(data.getColumns().size(), true);
                        table.appendRow(data.getColumns());
                        long k = data.getTotal();
                        int i = 0;
                        while (data.next()) {
                            /*for (String s : data.getDatas().get(i)) {
                                out.append(s).append(" | ");
                            }
                            out.append("\n");*/
                            table.appendRow(data.getRow());

                        }
                        out.append(table.toString());
                        out.flush();
                    }

                } else {
                    out.append("无效的命令\n").flush();
                }
            } catch (Exception e) {
                logger.error("run fail", e);
                out.append(e.getMessage()).append("\n");
                out.flush();
            }

        }
        while (line != null && !"exist".equals(line));
    }

}
