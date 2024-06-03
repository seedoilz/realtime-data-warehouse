package com.seedoilz.gmall.realtime.common.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class IkUtil {
    public static List<String> IKSplit(String keywords) {
        List<String> res = new ArrayList<>();
        StringReader stringReader = new StringReader(keywords);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
        try {
            Lexeme next = ikSegmenter.next();
            while (next != null){
                res.add(next.getLexemeText());
                next = ikSegmenter.next();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return res;
    }
}
