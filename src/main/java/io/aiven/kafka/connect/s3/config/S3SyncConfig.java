/*
 * Copyright (C) 2020 Aiven Oy
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.kafka.connect.s3.config;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.connect.common.config.AivenCommonConfig;
import io.aiven.kafka.connect.common.config.CompressionType;
import io.aiven.kafka.connect.common.config.OutputField;
import io.aiven.kafka.connect.common.config.OutputFieldEncodingType;
import io.aiven.kafka.connect.common.config.OutputFieldType;
import io.aiven.kafka.connect.common.config.TimestampSource;
import io.aiven.kafka.connect.common.config.validators.FileCompressionTypeValidator;
import io.aiven.kafka.connect.common.config.validators.NonEmptyPassword;
import io.aiven.kafka.connect.common.config.validators.OutputFieldsValidator;
import io.aiven.kafka.connect.common.config.validators.TimeZoneValidator;
import io.aiven.kafka.connect.common.config.validators.TimestampSourceValidator;
import io.aiven.kafka.connect.common.config.validators.UrlValidator;
import io.aiven.kafka.connect.common.templating.Template;

import com.amazonaws.regions.Regions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.AWS_ACCESS_KEY_ID;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.AWS_S3_BUCKET;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.AWS_S3_ENDPOINT;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.AWS_S3_PREFIX;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.AWS_S3_REGION;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.AWS_SECRET_ACCESS_KEY;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION_TYPE_GZIP;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.OUTPUT_COMPRESSION_TYPE_NONE;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.OUTPUT_FIELDS;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_KEY;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_OFFSET;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_TIMESTAMP;
import static io.aiven.kafka.connect.s3.config.AivenKafkaConnectS3Constants.OUTPUT_FIELD_NAME_VALUE;

public class S3SyncConfig extends AivenCommonConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3SyncConfig.class);

    //FIXME since we support so far both old style and new style of property names
    //      Importance was set to medium,
    //      as soon we will migrate to new values it must be set to HIGH
    //      same for default value
    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    public static final String AWS_S3_BUCKET_NAME_CONFIG = "aws.s3.bucket.name";
    public static final String AWS_S3_ENDPOINT_CONFIG = "aws.s3.endpoint";
    public static final String AWS_S3_PREFIX_CONFIG = "aws.s3.prefix";
    public static final String AWS_S3_REGION_CONFIG = "aws.s3.region";

    public static final String TIMESTAMP_TIMEZONE = "timestamp.timezone";
    public static final String TIMESTAMP_SOURCE = "timestamp.source";

    public S3SyncConfig(final Map<?, ?> originals) {
        super(configDef(), originals);
        validate();
    }

    public static ConfigDef configDef() {
        final var configDef = new ConfigDef();
        addAwsConfiguration(configDef);
        //so far it is null, since old and new variables now supported
        addCompressionTypeConfig(configDef, null);
        //so far it is null, since old and new variables now supported
        addOutputFieldsFormatConfigGroup(configDef, null);
        addTimestampConfig(configDef);
        addDeprecatedConfiguration(configDef);
        return configDef;
    }

    private static void addAwsConfiguration(final ConfigDef configDef) {
        configDef.define(
            AWS_ACCESS_KEY_ID_CONFIG,
            Type.PASSWORD,
            null,
            new NonEmptyPassword(),
            Importance.MEDIUM,
            "AWS Access Key ID"
        );

        configDef.define(
            AWS_SECRET_ACCESS_KEY_CONFIG,
            Type.PASSWORD,
            null,
            new NonEmptyPassword(),
            Importance.MEDIUM,
            "AWS Secret Access Key"
        );

        configDef.define(
            AWS_S3_BUCKET_NAME_CONFIG,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "AWS S3 Bucket name"
        );

        configDef.define(
            AWS_S3_ENDPOINT_CONFIG,
            Type.STRING,
            null,
            new UrlValidator(),
            Importance.LOW,
            "Explicit AWS S3 Endpoint Address, mainly for testing"
        );

        configDef.define(
            AWS_S3_REGION_CONFIG,
            Type.STRING,
            null,
            new AwsRegionValidator(),
            Importance.MEDIUM,
            "AWS S3 Region, e.g. us-east-1"
        );

        configDef.define(
            AWS_S3_PREFIX_CONFIG,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString(),
            Importance.MEDIUM,
            "Prefix for stored objects, e.g. cluster-1/"
        );

    }

    private static void addTimestampConfig(final ConfigDef configDef) {

        configDef.define(
            TIMESTAMP_TIMEZONE,
            Type.STRING,
            ZoneOffset.UTC.toString(),
            new TimeZoneValidator(),
            Importance.LOW,
            "Specifies the timezone in which the dates and time for the timestamp variable will be treated. "
                + "Use standard shot and long names. Default is UTC"
        );

        configDef.define(
            TIMESTAMP_SOURCE,
            Type.STRING,
            TimestampSource.Type.WALLCLOCK.name(),
            new TimestampSourceValidator(),
            Importance.LOW,
            "Specifies the the timestamp variable source. Default is wall-clock."
        );

    }

    private static void addDeprecatedConfiguration(final ConfigDef configDef) {
        configDef.define(
            AWS_ACCESS_KEY_ID,
            Type.PASSWORD,
            null,
            new NonEmptyPassword() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    LOGGER.info(AWS_ACCESS_KEY_ID
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, value);
                }
            },
            Importance.MEDIUM,
            "AWS Access Key ID"
        );

        configDef.define(
            AWS_SECRET_ACCESS_KEY,
            Type.PASSWORD,
            null,
            new NonEmptyPassword() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    LOGGER.info(AWS_SECRET_ACCESS_KEY
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, value);
                }
            },
            Importance.MEDIUM,
            "AWS Secret Access Key"
        );

        configDef.define(
            AWS_S3_BUCKET,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString() {
                @Override
                public void ensureValid(final String name, final Object o) {
                    LOGGER.info(AWS_S3_BUCKET
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, o);
                }
            },
            Importance.MEDIUM,
            "AWS S3 Bucket name"
        );

        configDef.define(
            AWS_S3_ENDPOINT,
            Type.STRING,
            null,
            new UrlValidator() {
                @Override
                public void ensureValid(final String name, final Object o) {
                    LOGGER.info(AWS_S3_ENDPOINT
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, o);
                }
            },
            Importance.LOW,
            "Explicit AWS S3 Endpoint Address, mainly for testing"
        );

        configDef.define(
            AWS_S3_REGION,
            Type.STRING,
            null,
            new AwsRegionValidator() {
                @Override
                public void ensureValid(final String name, final Object o) {
                    LOGGER.info(AWS_S3_REGION
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, o);
                }
            },
            Importance.MEDIUM,
            "AWS S3 Region, e.g. us-east-1"
        );

        configDef.define(
            AWS_S3_PREFIX,
            Type.STRING,
            null,
            new ConfigDef.NonEmptyString() {
                @Override
                public void ensureValid(final String name, final Object o) {
                    LOGGER.info(AWS_S3_PREFIX
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, o);
                }
            },
            Importance.MEDIUM,
            "Prefix for stored objects, e.g. cluster-1/"
        );

        configDef.define(
            OUTPUT_FIELDS,
            Type.LIST,
            OutputFieldType.VALUE.name,
            new OutputFieldsValidator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    LOGGER.info(OUTPUT_FIELDS
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, value);
                }
            },
            Importance.MEDIUM,
            "Output fields. A comma separated list of one or more: "
                + OUTPUT_FIELD_NAME_KEY + ", "
                + OUTPUT_FIELD_NAME_OFFSET + ", "
                + OUTPUT_FIELD_NAME_TIMESTAMP + ", "
                + OUTPUT_FIELD_NAME_VALUE
        );

        configDef.define(
            OUTPUT_COMPRESSION,
            Type.STRING,
            CompressionType.GZIP.name,
            new FileCompressionTypeValidator() {
                @Override
                public void ensureValid(final String name, final Object value) {
                    LOGGER.info(OUTPUT_COMPRESSION
                        + " property is deprecated please read documentation for the new name");
                    super.ensureValid(name, value);
                }
            },
            Importance.MEDIUM,
            "Output compression. Valid values are: "
                + OUTPUT_COMPRESSION_TYPE_GZIP + " and "
                + OUTPUT_COMPRESSION_TYPE_NONE
        );

    }

    private void validate() {
        if (Objects.isNull(getPassword(AWS_ACCESS_KEY_ID_CONFIG))
            && Objects.isNull(getPassword(AWS_ACCESS_KEY_ID))) {
            throw new ConfigException(
                String.format(
                    "Neither %s nor %s properties have been set",
                    AWS_ACCESS_KEY_ID_CONFIG,
                    AWS_ACCESS_KEY_ID)
            );
        } else if (Objects.isNull(getPassword(AWS_SECRET_ACCESS_KEY_CONFIG))
            && Objects.isNull(getPassword(AWS_SECRET_ACCESS_KEY))) {
            throw new ConfigException(
                String.format(
                    "Neither %s nor %s properties have been set",
                    AWS_SECRET_ACCESS_KEY_CONFIG,
                    AWS_SECRET_ACCESS_KEY)
            );
        } else if (Objects.isNull(getString(AWS_S3_BUCKET_NAME_CONFIG))
            && Objects.isNull(getString(AWS_S3_BUCKET))) {
            throw new ConfigException(
                String.format(
                    "Neither %s nor %s properties have been set",
                    AWS_S3_BUCKET_NAME_CONFIG,
                    AWS_S3_BUCKET)
            );
        }
    }

    public Password getAwsAccessKeyId() {
        //we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        return Objects.nonNull(getPassword(AWS_ACCESS_KEY_ID_CONFIG))
            ? getPassword(AWS_ACCESS_KEY_ID_CONFIG)
            : getPassword(AWS_ACCESS_KEY_ID);
    }

    public Password getAwsSecretKey() {
        //we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        return Objects.nonNull(getPassword(AWS_SECRET_ACCESS_KEY_CONFIG))
            ? getPassword(AWS_SECRET_ACCESS_KEY_CONFIG)
            : getPassword(AWS_SECRET_ACCESS_KEY);
    }

    public String getAwsEndPoint() {
        return Objects.nonNull(getString(AWS_S3_ENDPOINT_CONFIG))
            ? getString(AWS_S3_ENDPOINT_CONFIG)
            : getString(AWS_S3_ENDPOINT);
    }

    public Regions getRegion() {
        //we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        if (Objects.nonNull(getString(AWS_S3_REGION_CONFIG))) {
            return Regions.fromName(getString(AWS_S3_REGION_CONFIG));
        } else if (Objects.nonNull(getString(AWS_S3_REGION))) {
            return Regions.fromName(getString(AWS_S3_REGION));
        } else {
            return Regions.US_EAST_1;
        }
    }

    public String getBucketName() {
        return Objects.nonNull(getString(AWS_S3_BUCKET_NAME_CONFIG))
            ? getString(AWS_S3_BUCKET_NAME_CONFIG)
            : getString(AWS_S3_BUCKET);
    }

    public String getPrefix() {
        if (Objects.nonNull(getString(AWS_S3_PREFIX_CONFIG))) {
            return getString(AWS_S3_PREFIX_CONFIG);
        } else if (Objects.nonNull(getString(AWS_S3_PREFIX))) {
            return getString(AWS_S3_PREFIX);
        } else {
            return "";
        }
    }

    @Override
    public CompressionType getCompressionType() {
        //we have priority of properties if old one not set or both old and new one set
        // the new property value will be selected
        //default value is GZIP
        if (Objects.nonNull(getString(FILE_COMPRESSION_TYPE_CONFIG))) {
            return super.getCompressionType();
        } else if (Objects.nonNull(getString(OUTPUT_COMPRESSION))) {
            return CompressionType.forName(getString(OUTPUT_COMPRESSION));
        } else {
            return CompressionType.GZIP;
        }
    }

    @Override
    public List<OutputField> getOutputFields() {
        if (Objects.nonNull(getList(FORMAT_OUTPUT_FIELDS_CONFIG))) {
            return super.getOutputFields();
        } else if (Objects.nonNull(getList(OUTPUT_FIELDS))) {
            return getList(OUTPUT_FIELDS)
                .stream()
                .map(fieldName -> {
                    final var type = OutputFieldType.forName(fieldName);
                    final var encoding =
                        (type == OutputFieldType.KEY || type == OutputFieldType.VALUE)
                            ? getOutputFieldEncodingType()
                            : OutputFieldEncodingType.NONE;
                    return new OutputField(type, encoding);
                })
                .collect(Collectors.toUnmodifiableList());
        } else {
            return List.of(new OutputField(OutputFieldType.VALUE, OutputFieldEncodingType.BASE64));
        }
    }

    @Override
    public OutputFieldEncodingType getOutputFieldEncodingType() {
        return Objects.nonNull(getString(FORMAT_OUTPUT_FIELDS_VALUE_ENCODING_CONFIG))
            ? super.getOutputFieldEncodingType()
            : OutputFieldEncodingType.BASE64;
    }

    public Template getPrefixTemplate() {
        final var t = Template.of(getPrefix());
        t.instance()
            .bindVariable(
                "utc_date",
                () -> {
                    LOGGER.info("utc_date variable is deprecated please read documentation for the new name");
                    return "";
                })
            .bindVariable(
                "local_date",
                () -> {
                    LOGGER.info("local_date variable is deprecated please read documentation for the new name");
                    return "";
                })
            .render();
        return t;
    }

    public final ZoneId getTimezone() {
        return ZoneId.of(getString(TIMESTAMP_TIMEZONE));
    }

    public TimestampSource getTimestampSource() {
        return TimestampSource.of(
            getTimezone(),
            TimestampSource.Type.of(getString(TIMESTAMP_SOURCE))
        );
    }

    protected static class AwsRegionValidator implements ConfigDef.Validator {

        private static final String SUPPORTED_AWS_REGIONS =
            Arrays.stream(Regions.values())
                .map(Regions::getName)
                .collect(Collectors.joining(", "));

        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.nonNull(value)) {
                final String valueStr = (String) value;
                try {
                    Regions.fromName(valueStr);
                } catch (final IllegalArgumentException e) {
                    throw new ConfigException(
                        name, valueStr,
                        "supported values are: " + SUPPORTED_AWS_REGIONS);
                }
            }
        }

    }

}
