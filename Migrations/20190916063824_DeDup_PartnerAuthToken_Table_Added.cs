using System;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class DeDup_PartnerAuthToken_Table_Added : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "PartnerAuthTokens",
                schema: "dedup",
                columns: table => new
                {
                    auth_id = table.Column<string>(nullable: false),
                    oauth_code = table.Column<string>(nullable: true),
                    oauth_type = table.Column<int>(nullable: false),
                    oauth_expired_in = table.Column<DateTime>(nullable: true),
                    access_token = table.Column<string>(nullable: true),
                    refresh_token = table.Column<string>(nullable: true),
                    token_type = table.Column<string>(nullable: true),
                    expires_in = table.Column<DateTime>(nullable: true),
                    user_id = table.Column<string>(nullable: true),
                    session_nonce = table.Column<string>(nullable: true),
                    redirect_url = table.Column<string>(nullable: true),
                    created_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()"),
                    updated_at = table.Column<DateTime>(nullable: false, defaultValueSql: "now()")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_PartnerAuthTokens", x => x.auth_id);
                    table.ForeignKey(
                        name: "FK_PartnerAuthTokens_Resources_auth_id",
                        column: x => x.auth_id,
                        principalSchema: "dedup",
                        principalTable: "Resources",
                        principalColumn: "uuid",
                        onDelete: ReferentialAction.Cascade);
                });
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "PartnerAuthTokens",
                schema: "dedup");
        }
    }
}
