create or replace function public.current_app_role()
returns text
language sql
stable
security definer
set search_path = public
as $$
  select coalesce(
    (
      select p.role
      from public.app_user_profile p
      where p.user_id = auth.uid()
      limit 1
    ),
    ''
  );
$$;

create or replace function public.is_admin()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.current_app_role() = 'admin';
$$;

create or replace function public.has_content_role(allowed_roles text[])
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select public.current_app_role() = any(allowed_roles);
$$;

alter table public.app_user_profile
  alter column role set default 'viewer';

revoke all on function public.current_app_role() from public;
grant execute on function public.current_app_role() to authenticated;

revoke all on function public.is_admin() from public;
grant execute on function public.is_admin() to authenticated;

revoke all on function public.has_content_role(text[]) from public;
grant execute on function public.has_content_role(text[]) to authenticated;
