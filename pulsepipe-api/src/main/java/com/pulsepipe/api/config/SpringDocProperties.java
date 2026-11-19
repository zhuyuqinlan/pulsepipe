package com.pulsepipe.api.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * springdoc.info 配置项，用于填充 OpenAPI 文档的 Info 信息。
 * （项目未引入 Lombok，手写 getter/setter）
 */
@ConfigurationProperties(prefix = "springdoc.info")
public class SpringDocProperties {

    /** 文档标题 */
    private String title;
    /** 文档描述 */
    private String description;
    /** 接口版本 */
    private String version;
    /** 服务条款地址 */
    private String termsOfService;
    /** 文档摘要 */
    private String summary;
    /** 联系方式 */
    private Contact contact = new Contact();
    /** 许可证信息 */
    private License license = new License();

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getTermsOfService() {
        return termsOfService;
    }

    public void setTermsOfService(String termsOfService) {
        this.termsOfService = termsOfService;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public Contact getContact() {
        return contact;
    }

    public void setContact(Contact contact) {
        this.contact = contact;
    }

    public License getLicense() {
        return license;
    }

    public void setLicense(License license) {
        this.license = license;
    }

    /** 联系方式嵌套配置 */
    public static class Contact {

        /** 联系人名称 */
        private String name;
        /** 联系地址 */
        private String url;
        /** 联系邮箱 */
        private String email;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }
    }

    /** 许可证嵌套配置 */
    public static class License {

        /** 许可证名称 */
        private String name;
        /** 许可证地址 */
        private String url;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }
}
