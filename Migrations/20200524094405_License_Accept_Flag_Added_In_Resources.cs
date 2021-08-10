using Microsoft.EntityFrameworkCore.Migrations;

namespace Dedup.Migrations
{
    public partial class License_Accept_Flag_Added_In_Resources : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<bool>(
                name: "is_license_accepted",
                schema: "dedup",
                table: "Resources",
                nullable: true);
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "is_license_accepted",
                schema: "dedup",
                table: "Resources");
        }
    }
}