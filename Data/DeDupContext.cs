using Microsoft.EntityFrameworkCore;
using Dedup.Models;


namespace Dedup.Data
{
    public class DeDupContext : DbContext
    {
        public DeDupContext(DbContextOptions<DeDupContext> options) : base(options)
        {
        }

        //public DeDupContext() { }

        public DbSet<Resources> Resources { get; set; }
        public DbSet<DeDupSettings> DeDupSettings { get; set; }
        public DbSet<Connectors> Connectors { get; set; }
        public DbSet<AuthTokens> AuthTokens { get; set; }
        public DbSet<PartnerAuthTokens> PartnerAuthTokens { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseNpgsql(
        Dedup.Common.ConfigVars.Instance.connectionString,
        x => x.MigrationsHistoryTable("__MyMigrationsHistory", Dedup.Common.Constants.ADDON_DB_DEFAULT_SCHEMA));

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            //Assign default schema
            modelBuilder.HasDefaultSchema(Dedup.Common.Constants.ADDON_DB_DEFAULT_SCHEMA);

            //Assign default values of datetime columns
            modelBuilder.Entity<Resources>(entity => { entity.Property(e => e.updated_at).HasDefaultValueSql("now()"); });
            modelBuilder.Entity<Resources>(entity => { entity.Property(e => e.created_at).HasDefaultValueSql("now()"); });

            modelBuilder.Entity<DeDupSettings>(entity => { entity.Property(e => e.updated_at).HasDefaultValueSql("now()"); });
            modelBuilder.Entity<DeDupSettings>(entity => { entity.Property(e => e.created_at).HasDefaultValueSql("now()"); });

            //Assign composite primary keys on Connectors table
            modelBuilder.Entity<Connectors>().HasKey(s => new
            {
                s.connector_id,
                s.ccid
            });
            modelBuilder.Entity<Connectors>().HasIndex(p => new { p.connector_id, p.ccid }).IsUnique();

            //Assign default values of datetime columns
            modelBuilder.Entity<Connectors>(entity => { entity.Property(e => e.updated_at).HasDefaultValueSql("now()"); });
            modelBuilder.Entity<Connectors>(entity => { entity.Property(e => e.created_at).HasDefaultValueSql("now()"); });

            modelBuilder.Entity<AuthTokens>(entity => { entity.Property(e => e.updated_at).HasDefaultValueSql("now()"); });
            modelBuilder.Entity<AuthTokens>(entity => { entity.Property(e => e.created_at).HasDefaultValueSql("now()"); });

            modelBuilder.Entity<PartnerAuthTokens>(entity => { entity.Property(e => e.updated_at).HasDefaultValueSql("now()"); });
            modelBuilder.Entity<PartnerAuthTokens>(entity => { entity.Property(e => e.created_at).HasDefaultValueSql("now()"); });

            base.OnModelCreating(modelBuilder);
        }
    }
}
