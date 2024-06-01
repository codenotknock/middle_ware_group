package com.xiaofu.es.constants;

/**
 * @author fuzhouling
 * @date 2024/06/01
 * @program middle_ware_group
 * @description HotelConstants
 **/
public class HotelConstants {
    public static final String MAPPING_TEMPLATE = "{\"properties\": {\n" +
        "      \"name\": {\n" +
        "        \"type\": \"text\"\n" +
        "      },\n" +
        "      \"stars\": {\n" +
        "        \"type\": \"integer\"\n" +
        "      },\n" +
        "      \"price\": {\n" +
        "        \"type\": \"double\"\n" +
        "      },\n" +
        "      \"description\": {\n" +
        "        \"type\": \"text\"\n" +
        "      },\n" +
        "      \"address\": {\n" +
        "        \"type\": \"text\"\n" +
        "      },\n" +
        "      \"location\": {\n" +
        "        \"type\": \"geo_point\"\n" +
        "      },\n" +
        "      \"rooms\": {\n" +
        "        \"type\": \"nested\",\n" +
        "        \"properties\": {\n" +
        "          \"room_number\": {\n" +
        "            \"type\": \"keyword\"\n" +
        "          },\n" +
        "          \"room_type\": {\n" +
        "            \"type\": \"text\"\n" +
        "          },\n" +
        "          \"bed_count\": {\n" +
        "            \"type\": \"integer\"\n" +
        "          }\n" +
        "        }\n" +
        "      },\n" +
        "      \"amenities\": {\n" +
        "        \"type\": \"keyword\"\n" +
        "      },\n" +
        "      \"last_updated\": {\n" +
        "        \"type\": \"date\",\n" +
        "        \"format\": \"strict_date_time\"\n" +
        "      }\n" +
        "    }}";

}
