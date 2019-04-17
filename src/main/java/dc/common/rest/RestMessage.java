package dc.common.rest;

import java.io.Serializable;

/**
 * 
 * Created by jiangbaining
 * Date:2018/3/19
 */
public class RestMessage implements Serializable {


    //private boolean success=true;

    private int code=0;//0成功

    private String msg="操作成功";

    private Object data;




    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
