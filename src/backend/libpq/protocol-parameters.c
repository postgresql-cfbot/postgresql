/*-------------------------------------------------------------------------
 *
 * protocol-parameters.c
 *	  Routines to handle parsing and changing of protocol parameters.
 *
 * Portions Copyright (c) 2024, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/libpq/protocol-parameters.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "libpq/libpq-be.h"
#include "libpq/pqformat.h"
#include "tcop/tcopprot.h"
#include "utils/memutils.h"
#include "utils/guc.h"

static void SendSetProtocolParameterComplete(ProtocolParameter *param, bool error);


static MemoryContext ProtocolParameterMemoryContext;

struct ProtocolParameter SupportedProtocolParameters[] = {
	{
		"report_parameters",
		"",
		report_parameters_handler,
		.supported_string = "L",
	}
};

ProtocolParameter *
find_protocol_parameter(const char *name)
{
	for (ProtocolParameter *param = SupportedProtocolParameters; param->name; param++)
	{
		if (strcmp(param->name, name) == 0)
		{
			return param;
		}
	}
	return NULL;
}

void
init_protocol_parameter(ProtocolParameter *param, const char *value)
{
	const char *new_value = param->handler(param, value);

	/* If the handler returns NULL, use the default */
	if (!new_value)
		new_value = param->value;

	if (!ProtocolParameterMemoryContext)
		ProtocolParameterMemoryContext = AllocSetContextCreate(TopMemoryContext,
															   "ProtocolParameterMemoryContext",
															   ALLOCSET_DEFAULT_SIZES);

	param->value = MemoryContextStrdup(ProtocolParameterMemoryContext, new_value);

	param->requested = true;
}


void
set_protocol_parameter(const char *name, const char *value)
{
	ProtocolParameter *param = find_protocol_parameter(name);
	const char *new_value;

	if (!param)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unrecognized protocol parameter \"%s\"", name)));
	}
	if (!param->requested)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("protocol parameter \"%s\" was not requested during connection startup", name)));
	}
	new_value = param->handler(param, value);

	if (new_value)
	{
		char	   *copy = MemoryContextStrdup(ProtocolParameterMemoryContext, new_value);

		pfree(param->value);
		param->value = copy;
	}

	if (whereToSendOutput == DestRemote)
		SendSetProtocolParameterComplete(param, !new_value);
}

/*
 * Send a NegotiateProtocolParameter message to the client. This lets the
 * client know what values are accepted when changing the given parameter in
 * the future, as well as the parameter its current value.
 */
void
SendNegotiateProtocolParameter(ProtocolParameter *param)
{
	StringInfoData buf;

	pq_beginmessage(&buf, PqMsg_NegotiateProtocolParameter);
	pq_sendstring(&buf, param->name);
	pq_sendstring(&buf, param->value);
	if (param->supported_string)
		pq_sendstring(&buf, param->supported_string);
	else
		pq_sendstring(&buf, param->supported_handler());
	pq_endmessage(&buf);

	/* no need to flush, some other message will follow */
}

static void
SendSetProtocolParameterComplete(ProtocolParameter *param, bool error)
{
	StringInfoData buf;

	pq_beginmessage(&buf, PqMsg_SetProtocolParameterComplete);
	pq_sendstring(&buf, param->name);
	pq_sendstring(&buf, param->value);
	pq_sendbyte(&buf, error ? 'E' : 'S');
	pq_endmessage(&buf);
}
