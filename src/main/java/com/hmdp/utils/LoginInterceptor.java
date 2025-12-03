package com.hmdp.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@Component
@Slf4j
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1. 判断是否需要拦截，即threadlocal是否有用户
        if (UserHolder.getUser() == null) {
            log.debug("用户未登录，请求路径：{}", request.getRequestURI());
            // 设置响应状态码
            response.setStatus(401);
            // 设置响应内容类型
            response.setContentType("application/json;charset=UTF-8");
            // 构造响应内容
            String responseJson = "{\"success\":false,\"errorMsg\":\"用户未登录，请先登录\"}";
            // 写入响应
            response.getWriter().write(responseJson);
            return false;
        }
        return true;
    }
}
