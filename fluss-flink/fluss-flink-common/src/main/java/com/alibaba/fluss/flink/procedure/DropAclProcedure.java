package com.alibaba.fluss.flink.procedure;

import com.alibaba.fluss.security.acl.AccessControlEntryFilter;
import com.alibaba.fluss.security.acl.AclBindingFilter;
import com.alibaba.fluss.security.acl.FlussPrincipal;
import com.alibaba.fluss.security.acl.OperationType;
import com.alibaba.fluss.security.acl.PermissionType;
import com.alibaba.fluss.security.acl.Resource;
import com.alibaba.fluss.security.acl.ResourceFilter;

import org.apache.flink.table.procedure.ProcedureContext;

import java.util.Collections;

/** Procedure to drop acl. */
public class DropAclProcedure extends AbstractAclProcedure {
    public String[] call(
            ProcedureContext context,
            String resource,
            String permission,
            String principal,
            String operation)
            throws Exception {
        return call(context, resource, permission, principal, operation, null);
    }

    @Override
    protected String[] aclOperation(
            Resource resource,
            PermissionType permission,
            FlussPrincipal flussPrincipal,
            OperationType operationType,
            String host)
            throws Exception {
        admin.dropAcls(
                        Collections.singletonList(
                                new AclBindingFilter(
                                        new ResourceFilter(resource.getType(), resource.getName()),
                                        new AccessControlEntryFilter(
                                                flussPrincipal, host, operationType, permission))))
                .all()
                .get();
        return new String[] {"success"};
    }
}
