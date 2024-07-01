package com.other;

import com.alibaba.fastjson.parser.Feature;
import com.alibaba.fastjson.parser.ParserConfig;

/**
 * Created by wuqing on 17/4/26.
 */
public class JSON extends com.alibaba.fastjson.JSON {

    public static <T> T parseObject2(String text, TypeReferenceMogu type, Feature... features) {
        return parseObject(text, type.getType(), ParserConfig.global, DEFAULT_PARSER_FEATURE, features);
    }

}
