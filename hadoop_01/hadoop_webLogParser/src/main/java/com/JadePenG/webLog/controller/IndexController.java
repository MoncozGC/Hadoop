package com.JadePenG.webLog.controller;

import com.JadePenG.webLog.pojo.FlowReturnPojo;
import com.JadePenG.webLog.service.AvgPvService;
import com.JadePenG.webLog.service.FlowService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * @author Peng
 * @Description
 */
@Controller
public class IndexController {

    @Autowired
    private AvgPvService avgPvService;

    @Autowired
    private FlowService flowService;

    @RequestMapping("/index.action")
    public String skipToIndex(){

        return "index";
    }

    @RequestMapping("/avgPvNum.action")
    @ResponseBody
    public  String getAvgPvJson(){
        String avgJson =  avgPvService.getAvgJson();
        return avgJson;
    }


    @RequestMapping("/flowNum.action")
    @ResponseBody
    public FlowReturnPojo getFlowNum(){

        FlowReturnPojo flowReturnPojo =  flowService.getAllFlowNum();

        return flowReturnPojo;
    }

}
