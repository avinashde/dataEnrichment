package com.org.enrich;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PsqlProperties {
    public List<String> getProp() throws IOException {
        FileInputStream fis = new FileInputStream("psql.properties");
        Properties p = new Properties();
        p.load(fis);
        String connString = (String) p.get("Connstring");
        String username = (String) p.get("Uname");
        String password = (String) p.get("Password");
        String driver = (String) p.get("Driver");
        List<String> list = new ArrayList<String>();
        list.add(connString);
        list.add(username);
        list.add(password);
        list.add(driver);
        return list;
    }
}
