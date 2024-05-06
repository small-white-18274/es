package cn.itcast.hotel.pojo;

import lombok.Data;

@Data
public class RequestParams {

    private String key;
    private String sortBy;
    private Integer page;
    private Integer size;


}
