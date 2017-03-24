package nablarch.fw.messaging.action.form;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @author hisaaki sioiri
 */
public class ErrorConstructorForm {

    public ErrorConstructorForm(Map<String, ?> data) {
        throw new NullPointerException("ぬるぽ");
    }
}
