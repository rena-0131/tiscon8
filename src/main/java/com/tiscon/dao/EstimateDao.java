package com.tiscon.dao;

import com.tiscon.domain.*;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.*;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import org.springframework.web.client.RestTemplate;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.List;
import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;

/**
 * 引越し見積もり機能においてDBとのやり取りを行うクラス。
 *
 * @author Oikawa Yumi
 */
@Component
public class EstimateDao {

    /** データベース・アクセスAPIである「JDBC」を使い、名前付きパラメータを用いてSQLを実行するクラス */
    private final NamedParameterJdbcTemplate parameterJdbcTemplate;

    /**
     * コンストラクタ。
     *
     * @param parameterJdbcTemplate NamedParameterJdbcTemplateクラス
     */
    public EstimateDao(NamedParameterJdbcTemplate parameterJdbcTemplate) {
        this.parameterJdbcTemplate = parameterJdbcTemplate;
    }

    /**
     * 顧客テーブルに登録する。
     *
     * @param customer 顧客情報
     * @return 登録件数
     */
    public int insertCustomer(Customer customer) {
        String sql = "INSERT INTO CUSTOMER(OLD_PREFECTURE_ID, NEW_PREFECTURE_ID, CUSTOMER_NAME, TEL, EMAIL, OLD_ADDRESS, NEW_ADDRESS)"
                + " VALUES(:oldPrefectureId, :newPrefectureId, :customerName, :tel, :email, :oldAddress, :newAddress)";
        KeyHolder keyHolder = new GeneratedKeyHolder();
        int resultNum = parameterJdbcTemplate.update(sql, new BeanPropertySqlParameterSource(customer), keyHolder);
        customer.setCustomerId(keyHolder.getKey().intValue());
        return resultNum;
    }

    /**
     * オプションサービス_顧客テーブルに登録する。
     *
     * @param optionService オプションサービス_顧客に登録する内容
     * @return 登録件数
     */
    public int insertCustomersOptionService(CustomerOptionService optionService) {
        String sql = "INSERT INTO CUSTOMER_OPTION_SERVICE(CUSTOMER_ID, SERVICE_ID)"
                + " VALUES(:customerId, :serviceId)";
        return parameterJdbcTemplate.update(sql, new BeanPropertySqlParameterSource(optionService));
    }

    /**
     * 顧客_荷物テーブルに登録する。
     *
     * @param packages 登録する荷物
     * @return 登録件数
     */
    public int[] batchInsertCustomerPackage(List<CustomerPackage> packages) {
        String sql = "INSERT INTO CUSTOMER_PACKAGE(CUSTOMER_ID, PACKAGE_ID, PACKAGE_NUMBER)"
                + " VALUES(:customerId, :packageId, :packageNumber)";
        SqlParameterSource[] batch = SqlParameterSourceUtils.createBatch(packages.toArray());

        return parameterJdbcTemplate.batchUpdate(sql, batch);
    }

    /**
     * 都道府県テーブルに登録されているすべての都道府県を取得する。
     *
     * @return すべての都道府県
     */
    public List<Prefecture> getAllPrefectures() {
        String sql = "SELECT PREFECTURE_ID, PREFECTURE_NAME FROM PREFECTURE";
        return parameterJdbcTemplate.query(sql,
                BeanPropertyRowMapper.newInstance(Prefecture.class));
    }

    /**
     * 都道府県間の距離を取得する。
     *
     * @param prefectureIdFrom 引っ越し元の都道府県
     * @param prefectureIdTo   引越し先の都道府県
     * @return 距離[km]
     */
    public double getDistance(String prefectureIdFrom, String prefectureIdTo) {
        // 都道府県のFromとToが逆転しても同じ距離となるため、「そのままの状態のデータ」と「FromとToを逆転させたデータ」をくっつけた状態で距離を取得する。
        String sql = "SELECT DISTANCE FROM (" +
                "SELECT PREFECTURE_ID_FROM, PREFECTURE_ID_TO, DISTANCE FROM PREFECTURE_DISTANCE UNION ALL " +
                "SELECT PREFECTURE_ID_TO PREFECTURE_ID_FROM ,PREFECTURE_ID_FROM PREFECTURE_ID_TO ,DISTANCE FROM PREFECTURE_DISTANCE) " +
                "WHERE PREFECTURE_ID_FROM  = :prefectureIdFrom AND PREFECTURE_ID_TO  = :prefectureIdTo";

        PrefectureDistance prefectureDistance = new PrefectureDistance();
        prefectureDistance.setPrefectureIdFrom(prefectureIdFrom);
        prefectureDistance.setPrefectureIdTo(prefectureIdTo);

        double distance;
        try {
            distance = parameterJdbcTemplate.queryForObject(sql, new BeanPropertySqlParameterSource(prefectureDistance), double.class);
        } catch (IncorrectResultSizeDataAccessException e) {
            distance = 0;
        }
        return distance;
    }

    public String getPrefectureName (String prefectureId, String sql) {
        SqlParameterSource paramSource = new MapSqlParameterSource("prefectureId", prefectureId);
        String prefecture_name = parameterJdbcTemplate.queryForObject(sql, paramSource, String.class);
        return prefecture_name;
    }

    public List<String> getCoordinates(String address) throws ParserConfigurationException, SAXException, IOException {
        RestTemplate restTemplate = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_XML));
        headers.add("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36");
        HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);

        String url = "https://www.geocoding.jp/api/?q=" + address;
        //String xml = restTemplate.getForObject(url, String.class);
        ResponseEntity<String> response_xml = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
        String xml = response_xml.getBody();

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        InputSource is = new InputSource(new StringReader(xml));
        Document doc = builder.parse(is);

        Element coord = (Element) doc.getElementsByTagName("coordinate").item(0);

        String lat = coord.getElementsByTagName("lat").item(0).getTextContent();
        String lon = coord.getElementsByTagName("lon").item(0).getTextContent();

        return Arrays.asList(lon, lat);
    }

    public double getRouteDistance(List<String> old_coordinate, List<String> new_coordinate) {
        RestTemplate restTemplate = new RestTemplate();

        String api_key = "5b3ce3597851110001cf6248437e756c4ca94adaa987754b66129748";
        String url = "https://api.openrouteservice.org/v2/directions/driving-car?api_key=" + api_key 
                        + "&start="+ old_coordinate.get(0) + "," + old_coordinate.get(1) 
                        + "&end=" + new_coordinate.get(0) + "," + new_coordinate.get(1);

        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Arrays.asList(MediaType.APPLICATION_XML));
        headers.add("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36");
        HttpEntity<String> entity = new HttpEntity<String>("parameters", headers);

        ResponseEntity<String> response_json = restTemplate.exchange(url, HttpMethod.GET, entity, String.class);
        String json = response_json.getBody();

        // String json = restTemplate.getForObject(url, String.class);
        JSONObject route_data = new JSONObject(json);
        JSONArray features = route_data.getJSONArray("features");
        JSONObject distance = features.getJSONObject(0).getJSONObject("properties").getJSONObject("summary");

        return distance.getDouble("distance");
    }

    /**
     * 実際の距離を取得する。
     * 
     * @param prefectureIdFrom
     * @param oldAddress
     * @param prefectureIdTo
     * @param newAddress
     * @return 距離
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    public double getRealDistance (String prefectureIdFrom, String prefectureIdTo, String oldAddress, String newAddress) throws ParserConfigurationException, SAXException, IOException {
        String sql_old_prefecture = "SELECT PREFECTURE_NAME FROM PREFECTURE WHERE PREFECTURE_ID = " + prefectureIdFrom;
        String sql_new_prefecture = "SELECT PREFECTURE_NAME FROM PREFECTURE WHERE PREFECTURE_ID = " + prefectureIdTo;

        String old_prefecture_name = this.getPrefectureName(prefectureIdFrom, sql_old_prefecture); 
        String new_prefecture_name = this.getPrefectureName(prefectureIdTo, sql_new_prefecture); 
        
        System.out.println(old_prefecture_name);
        System.out.println(new_prefecture_name);

        String full_old_address = old_prefecture_name + oldAddress;
        String full_new_address = new_prefecture_name + newAddress;

        System.out.println(full_old_address);
        System.out.println(full_new_address);

        List<String> old_coord = this.getCoordinates(full_old_address);
        List<String> new_coord = this.getCoordinates(full_new_address);
        
        double distance = this.getRouteDistance(old_coord, new_coord);
        return distance;
    }

    /**
     * 荷物ごとの段ボール数を取得する。
     *
     * @param packageId 荷物ID
     * @return 段ボール数
     */
    public int getBoxPerPackage(int packageId) {
        String sql = "SELECT BOX FROM PACKAGE_BOX WHERE PACKAGE_ID = :packageId";

        SqlParameterSource paramSource = new MapSqlParameterSource("packageId", packageId);
        return parameterJdbcTemplate.queryForObject(sql, paramSource, Integer.class);
    }

    /**
     * 段ボール数に応じたトラック料金を取得する。
     *
     * @param boxNum 総段ボール数
     * @return 料金[円]
     */
    public int getPricePerTruck(int boxNum) {
        String sql = "SELECT PRICE FROM TRUCK_CAPACITY WHERE MAX_BOX >= :boxNum ORDER BY PRICE LIMIT 1";

        SqlParameterSource paramSource = new MapSqlParameterSource("boxNum", boxNum);
        return parameterJdbcTemplate.queryForObject(sql, paramSource, Integer.class);
    }

    /**
     * オプションサービスの料金を取得する。
     *
     * @param serviceId サービスID
     * @return 料金
     */
    public int getPricePerOptionalService(int serviceId) {
        String sql = "SELECT PRICE FROM OPTIONAL_SERVICE WHERE SERVICE_ID = :serviceId";

        SqlParameterSource paramSource = new MapSqlParameterSource("serviceId", serviceId);
        return parameterJdbcTemplate.queryForObject(sql, paramSource, Integer.class);
    }

    /**
     * 引越し予定日を考慮する料金。
     * 
     * @param plannedDate 引越し予定日
     * @return 季節係数
     */
    public double getSeasonFactor(LocalDate plannedDate) {

        int month = plannedDate.getMonthValue();

        if (month == 3 || month == 4) {
            return 1.5;
        } else if (month == 9) {
            return 1.2;
        } else {
            return 1;
        }
    }
}
