using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class DeDup_Init : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.EnsureSchema(
                name: "dedup");

            migrationBuilder.CreateTable(
                name: "Resources",
                schema: "dedup",
                columns: table => new
                {
                    uuid = table.Column<string>(nullable: false),
                    app_name = table.Column<string>(nullable: true),
                    callback_url = table.Column<string>(nullable: true),
                    created_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()"),
                    expired_at = table.Column<DateTime>(nullable: true),
                    heroku_id = table.Column<string>(nullable: true),
                    plan = table.Column<string>(nullable: true),
                    private_app_url = table.Column<string>(nullable: true),
                    region = table.Column<string>(nullable: true),
                    updated_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()"),
                    user_email = table.Column<string>(nullable: true),
                    user_name = table.Column<string>(nullable: true),
                    user_organization = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Resources", x => x.uuid);
                });

            migrationBuilder.CreateTable(
                name: "AuthTokens",
                schema: "dedup",
                columns: table => new
                {
                    auth_id = table.Column<string>(nullable: false),
                    access_token = table.Column<string>(nullable: false),
                    created_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()"),
                    expires_in = table.Column<DateTime>(nullable: false),
                    redirect_url = table.Column<string>(nullable: true),
                    refresh_token = table.Column<string>(nullable: false),
                    session_nonce = table.Column<string>(nullable: true),
                    token_type = table.Column<string>(nullable: true),
                    updated_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()"),
                    user_id = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AuthTokens", x => x.auth_id);
                    table.ForeignKey(
                        name: "FK_AuthTokens_Resources_auth_id",
                        column: x => x.auth_id,
                        principalSchema: "dedup",
                        principalTable: "Resources",
                        principalColumn: "uuid",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "DeDupSettings",
                schema: "dedup",
                columns: table => new
                {
                    ccid = table.Column<string>(nullable: false),
                    created_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()"),
                    database_config_json = table.Column<string>(nullable: true),
                    updated_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_DeDupSettings", x => x.ccid);
                    table.ForeignKey(
                        name: "FK_DeDupSettings_Resources_ccid",
                        column: x => x.ccid,
                        principalSchema: "dedup",
                        principalTable: "Resources",
                        principalColumn: "uuid",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Connectors",
                schema: "dedup",
                columns: table => new
                {
                    connector_id = table.Column<int>(nullable: false),
                    ccid = table.Column<string>(nullable: false),
                    connector_name = table.Column<string>(nullable: true),
                    connector_type = table.Column<int>(nullable: false),
                    created_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()"),
                    custom_schedule_in_minutes = table.Column<int>(nullable: true),
                    dest_config_json = table.Column<string>(nullable: true),
                    dest_object_name = table.Column<string>(nullable: true),
                    dest_schema = table.Column<string>(nullable: true),
                    job_id = table.Column<string>(nullable: true),
                    schedule_type = table.Column<int>(nullable: false),
                    src_config_json = table.Column<string>(nullable: true),
                    src_object_fields_json = table.Column<string>(nullable: true),
                    src_object_name = table.Column<string>(nullable: true),
                    src_schema = table.Column<string>(nullable: true),
                    sync_count = table.Column<int>(nullable: true),
                    sync_ended_at = table.Column<DateTime>(nullable: true),
                    sync_log_json = table.Column<string>(nullable: true),
                    sync_new_record_filter = table.Column<string>(nullable: true),
                    sync_src = table.Column<int>(nullable: false),
                    sync_started_at = table.Column<DateTime>(nullable: true),
                    sync_status = table.Column<int>(nullable: true),
                    sync_update_record_filter = table.Column<string>(nullable: true),
                    two_way_sync_priority = table.Column<int>(nullable: false),
                    updated_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Connectors", x => new { x.connector_id, x.ccid });
                    table.ForeignKey(
                        name: "FK_Connectors_DeDupSettings_ccid",
                        column: x => x.ccid,
                        principalSchema: "dedup",
                        principalTable: "DeDupSettings",
                        principalColumn: "ccid",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Connectors_ccid",
                schema: "dedup",
                table: "Connectors",
                column: "ccid");

            migrationBuilder.CreateIndex(
                name: "IX_Connectors_connector_id_ccid",
                schema: "dedup",
                table: "Connectors",
                columns: new[] { "connector_id", "ccid" },
                unique: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "AuthTokens",
                schema: "dedup");

            migrationBuilder.DropTable(
                name: "Connectors",
                schema: "dedup");

            migrationBuilder.DropTable(
                name: "DeDupSettings",
                schema: "dedup");

            migrationBuilder.DropTable(
                name: "Resources",
                schema: "dedup");
        }
    }
}
