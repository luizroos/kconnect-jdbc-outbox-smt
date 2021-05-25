package transform;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

public class TransformField {

	private final int orderInGroup;
	private final String documentation;
	private final String displayName;
	private final String name;
	private final Type type;
	private final Importance importance;
	private final String description;
	private final Width width;
	private final Object defaultValue;

	public TransformField(int orderInGroup, String documentation, String name, String displayName, String description,
			Type type, Importance importance, Width width, Object defaultValue) {
		this.orderInGroup = orderInGroup;
		this.displayName = displayName;
		this.name = name;
		this.type = type;
		this.importance = importance;
		this.description = description;
		this.width = width;
		this.defaultValue = defaultValue;
		this.documentation = documentation;
	}

	public void define(ConfigDef config) {
		config.define(name, //
				type, //
				defaultValue, //
				null, //
				importance, //
				description, //
				documentation, //
				orderInGroup, //
				width, //
				displayName, //
				Collections.emptyList(), //
				null);
	}

	public String getName() {
		return name;
	}

	public int getReqInteger(Map<String, ?> transformConfigMap) {
		return Integer.valueOf(getReqString(transformConfigMap));
	}

	public String getReqString(Map<String, ?> transformConfigMap) {
		final String value = getString(transformConfigMap);
		if (value == null) {
			throw new IllegalStateException(String.format("Field %s is required", name));
		}
		return value;
	}

	public String getString(Map<String, ?> transformConfigMap) {
		Object fieldValue = transformConfigMap.get(name);
		if (fieldValue == null) {
			fieldValue = defaultValue;
		}
		if (fieldValue == null) {
			return null;
		}
		return fieldValue.toString().trim();
	}

	public List<String> getList(final Map<String, ?> transformConfigMap) {
		Object fieldValue = transformConfigMap.get(name);
		if (fieldValue == null) {
			fieldValue = defaultValue;
		}
		if (fieldValue == null) {
			return null;
		}

		return Arrays.asList(fieldValue.toString().split(","));
	}
}
